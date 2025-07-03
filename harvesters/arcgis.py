# Standard library
import os
import time
import re
import csv
import json
import yaml
from urllib.parse import urlparse, parse_qs

# Third-party
import requests
import pandas as pd

# Project-specific
from utils.field_order import FIELD_ORDER, PRIMARY_FIELD_ORDER
from utils.distribution_writer import load_distribution_types, generate_secondary_table
from harvesters.base import BaseHarvester
from utils.cleaner import basic_cleaning, spatial_cleaning, validation_pipeline


class ArcGISHarvester(BaseHarvester):
    def __init__(self, config, schema):
        super().__init__(config, schema)
        self.config = config
        self.schema = schema
        self.distribution_types = load_distribution_types()

    def fetch(self):
        """
        Fetch raw data from a list of ArcGIS Hub URLs defined in a CSV.
        """
        hub_file = self.config.get("hub_list_csv")
        records = []
        with open(hub_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                hub_id = row['ID']
                url = row['Identifier']
                try:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    print(f"[ArcGIS] Error fetching {hub_id}: {e}")
                    continue
                records.append({
                    'hub_id': hub_id,
                    'provider': row.get('Title', ''),
                    'spatial_coverage': row.get('Spatial Coverage', ''),
                    'is_part_of': row.get('ID', hub_id),
                    'member_of': row.get('Member Of', hub_id),
                    'title_source': row.get('Publisher', ''),
                    'default_bbox': row.get('Bounding Box', ''),
                    'raw_data': data
                })
        return records
        
    def normalize(self, fetched_records):
        """
        Flatten fetched ArcGIS records, parse fields, and return primary and secondary metadata.
        """
        df = self.flatten_datasets(fetched_records)
        df = (
            df
            .pipe(self.parse_identifiers)
            .pipe(self.format_titles)
            .pipe(self.clean_descriptions)
            .pipe(self.harvest_creators)
            .pipe(self.build_keyword_column)
            .pipe(self.harvest_dates)
            .pipe(self.compute_temporal_coverage)
            .pipe(self.compute_bbox_column)
            .pipe(self.build_distribution_columns)
            .pipe(self.add_base_fields)
            .pipe(self.clean_creator_values)
            .pipe(self.drop_rows_without_resource_class)
            
            
        )
        for col in df.columns:
            types = df[col].apply(type).value_counts()
            print(f"[DEBUG] Column {col} types:\n{types}\n")

        # Drop raw nested fields before cleaning
        df = df.drop(columns=['distributions', 'creator_info', 'keywords_list'], errors='ignore')

        df = (
            df
            .pipe(spatial_cleaning)
            .pipe(basic_cleaning)
            .pipe(validation_pipeline)

        )
    
        primary_records = df.to_dict(orient='records')
        secondary_df = generate_secondary_table(pd.DataFrame(primary_records), self.distribution_types)
        return primary_records, secondary_df.to_dict(orient='records')

    def harvest(self):
        """
        Full workflow: fetch data, normalize it, and write outputs.
        Returns dict of generated file paths.
        """
        print("[ArcGIS] Fetching data from hubs...")
        fetched = self.fetch()
        print(f"[ArcGIS] Fetched {len(fetched)} hub records. Normalizing...")
        primary, secondary = self.normalize(fetched)
        results = self.write_outputs(primary, secondary)
        print(f"[ArcGIS] Completed harvest: {results}")
        return results

    def flatten_datasets(self, records):
        rows = []
        for rec in records:
            hub_id = rec['hub_id']
            for ds in rec['raw_data'].get('dataset', []):
                rows.append({
                    'hub_id': hub_id,
                    'provider': rec.get('provider', ''),
                    'spatial_coverage': rec.get('spatial_coverage', ''),
                    'is_part_of': rec.get('is_part_of', hub_id),
                    'member_of': rec.get('member_of', hub_id),
                    'title_source': rec.get('title_source', ''),
                    'alternative_title': ds.get('title', 'Untitled'),
                    'description_raw': ds.get('description', ''),
                    'creator_info': ds.get('publisher', {}),
                    'keywords_list': ds.get('keyword', []),
                    'date_issued_raw': ds.get('issued', ''),
                    'date_modified_raw': ds.get('modified', ''),
                    'rights': ds.get('license', ''),
                    'identifier_raw': ds.get('identifier', ''),
                    'landing_page': ds.get('landingPage', ''),
                    'spatial': ds.get('spatial', {}),
                    'bbox_fallback': rec.get('default_bbox', ''),
                    'distributions': ds.get('distribution', []),
                })
        return pd.DataFrame(rows)

    def harvest_identifier_and_id(self, identifier: str) -> tuple:
        parsed = urlparse(identifier)
        qs = parse_qs(parsed.query)
        if 'id' in qs:
            ds_id = qs['id'][0]
            cleaned = f"https://hub.arcgis.com/datasets/{ds_id}"
            return cleaned, ds_id
        return identifier, identifier

    def parse_identifiers(self, df):
        ids = df['identifier_raw'].apply(self.harvest_identifier_and_id)
        df[['Identifier', 'ID']] = pd.DataFrame(ids.tolist(), index=df.index)
        return df

        
    def format_titles(self, df):
        def _format(row):
            alternative_title = row['alternative_title']
            title_source = row['title_source']
            year = ""
            year_range_match = re.search(r"\\b(\\d{4})-(\\d{4})\\b", alternative_title)
            single_year_match = re.search(r"\\b(17\\d{2}|18\\d{2}|19\\d{2}|20\\d{2})\\b", alternative_title)

            if year_range_match:
                year = f"{year_range_match.group(1)}-{year_range_match.group(2)}"
                alternative_title = alternative_title.replace(year, "").strip().rstrip(',')
            elif single_year_match:
                year = single_year_match.group(1)
                alternative_title = alternative_title.replace(year, "").strip().rstrip(',')

            title = f"{alternative_title} [{title_source}]"
            if year:
                title += f" {{{year}}}"
            return title

        df['Title'] = df.apply(_format, axis=1)
        return df


    def clean_descriptions(self, df):
        def _clean(text):
            text = text.replace("{{default.description}}", "").replace("{{description}}", "")
            text = re.sub(r'[\n\r]+', ' ', text)
            text = re.sub(r'\s{2,}', ' ', text)
            return text.translate({8217: "'", 8220: '"', 8221: '"', 160: "", 183: "", 8226: "", 8211: '-', 8203: ""})
        df['Description'] = df['description_raw'].apply(_clean)
        return df

    def harvest_creator(self, info):
        if isinstance(info, dict):
            for v in info.values():
                return v.replace(u"\u2019", "'")
        return ''

    def harvest_creators(self, df):
        df['Creator'] = df['creator_info'].apply(self.harvest_creator)
        return df

    def build_keyword_column(self, df):
        df['Keyword'] = df['keywords_list'].apply(lambda lst: '|'.join(lst).replace(' ', ''))
        return df

    def harvest_dates(self, df):
        df['Date Issued'] = df['date_issued_raw'].str.split('T').str[0]
        df['Date Modified'] = df['date_modified_raw'].str.split('T').str[0]
        return df

    def compute_temporal_coverage(self, df):
        def _cov(r):
            match = re.search(r"\{(.*?)\}", r['Title'])
            if match:
                tc = match.group(1)
                dr = tc if '-' in tc else f"{tc}-{tc}"
            else:
                tc = f"Last modified {r['Date Modified']}"
                dr = ''
            return pd.Series({'Temporal Coverage': tc, 'Date Range': dr})
        cov = df.apply(_cov, axis=1)
        return pd.concat([df, cov], axis=1)
        
    def clean_creator_values(self, df):
        def _clean(value):
            if isinstance(value, dict) and 'name' in value:
                return value['name']
            elif isinstance(value, str):
                match = re.match(r"\\{\\s*'name'\\s*:\\s*'(.+?)'\\s*\\}", value)
                if match:
                    return match.group(1)
                return value
            return value
    
        df['Creator'] = df['Creator'].apply(_clean)
        return df
        
    def drop_rows_without_resource_class(self, df):
        df['Resource Class'] = df['Resource Class'].astype(str).replace(r'^\s*$', pd.NA, regex=True)
        df = df.dropna(subset=['Resource Class'])
        return df


    def compute_bbox_column(self, df):
        def _bbox(r):
            sp = r.get('spatial', None)          # this is the raw 'spatial' field from ArcGIS dataset JSON
            fallback = r.get('bbox_fallback', '')  # default bbox from your input CSV

            # Case 1: if sp is a valid bbox string, return it directly
            if isinstance(sp, str) and sp.count(',') == 3:
                return sp

            # # Case 2: optional future support for dict-based bbox (unlikely, but safe)
            # if isinstance(sp, dict) and sp.get('type') == 'envelope' and 'coordinates' in sp:
            #     coords = sp['coordinates']
            #     rounded = [str(round(c, 3)) for pair in coords for c in pair]
            #     return ','.join(rounded)

            # Case 3: fallback to spreadsheet value if nothing else worked
            return fallback if pd.notnull(fallback) else ''

        df['Bounding Box'] = df.apply(_bbox, axis=1)
        return df


    def build_distribution_columns(self, df):
        dist_df = df.apply(
            lambda r: pd.Series({
                k: str(v) if not isinstance(v, (list, pd.Series)) and pd.notna(v) else ''
                for k, v in self.harvest_distribution_fields(r['distributions'], r['Title'], r['Description']).items()
            }),
            axis=1
        )
        df = pd.concat([df, dist_df], axis=1)

        # Ensure distribution columns are strictly scalars with empty strings
        distribution_cols = ['download', 'featureService', 'mapService', 'imageService', 'tileService']
        for col in distribution_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x if isinstance(x, str) else '')

        return df



    def harvest_distribution_fields(self, distributions, title: str, description: str) -> dict:
        """
        harvests distribution URLs and infers resource class, type, and format from distribution records.
        """
        distribution_fields = {
            'download': '',
            'featureService': '',
            'mapService': '',
            'imageService': '',
            'tileService': '',
            'Resource Class': '',
            'Resource Type': '',
            'Format': '',
        }
        for dist in distributions:
            dist_title = dist.get('title', '')
            access_url = dist.get('accessURL', '')
            if dist_title == 'Shapefile':
                distribution_fields['download'] = access_url
                distribution_fields['Resource Class'] = 'Datasets|Web services'
                distribution_fields['Format'] = 'Shapefile'
            if dist_title == 'ArcGIS GeoService' and access_url:
                if 'FeatureServer' in access_url:
                    distribution_fields['featureService'] = access_url
                    distribution_fields['Resource Class'] = 'Web services'
                elif 'MapServer' in access_url:
                    distribution_fields['mapService'] = access_url
                    distribution_fields['Resource Class'] = 'Web services'
                elif 'ImageServer' in access_url:
                    distribution_fields['imageService'] = access_url
                    distribution_fields['Resource Class'] = 'Imagery|Web services'
                    distribution_fields['Format'] = 'Imagery'
                    distribution_fields['Resource Type'] = 'Raster data'
                elif 'TileServer' in access_url:
                    distribution_fields['tileService'] = access_url
                    distribution_fields['Resource Class'] = 'Web services'
        if 'LiDAR' in title or 'LiDAR' in description:
            distribution_fields['Resource Type'] = 'LiDAR'
        return distribution_fields

    def add_base_fields(self, df):
        today = time.strftime('%Y-%m-%d')
        df['Code'] = df['hub_id']
        df['Provider'] = df['provider']
        df['Display Note'] = (
            "This dataset was automatically cataloged from the provider's ArcGIS Hub. "
            "In some cases, information shown here may be incorrect or out-of-date. "
            "Click the 'Visit Source' button to search for items on the original provider's website."
        )
        df['Language'] = 'eng'
        df['Access Rights'] = 'Public'
        df['Accrual Method'] = 'ArcGIS Hub'
        df['Date Accessioned'] = today
        df['Publication State'] = 'published'
        df['Is Part Of'] = df['is_part_of']
        df['Member Of'] = df['member_of']
        df['Spatial Coverage'] = df['spatial_coverage']
        df['Alternative Title'] = df['alternative_title']
        return df


def main():
    """
    Run ArcGIS harvestion standalone for local testing.
    """
    config_path = "config/arcgis.yaml"
    schema_path = "schemas/geobtaa_schema.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    Harvester = ArcGISHarvester(config, schema_path)
    Harvester.harvest()

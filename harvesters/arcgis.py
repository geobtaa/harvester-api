"""
ArcGIS Harvester

This module defines:
- ArcGISHarvester (inherits from BaseHarvester)
- ArcGIS-specific data parsing and field derivation functions
"""

import csv
import requests
import pandas as pd
import time
import re
from urllib.parse import urlparse, parse_qs

from harvesters.base import BaseHarvester
from utils.distribution_writer import load_distribution_types, generate_secondary_table
from utils.cleaner import spatial_cleaning
from utils.validation import validation_pipeline


class ArcGISHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)
        self.distribution_types = None

    def load_schema(self):
        # You could load the schema here too if needed
        self.distribution_types = load_distribution_types()

    def fetch(self):
        hub_file = self.config.get("hub_list_csv")
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
                    yield f"[ArcGIS] ❌ Error fetching {hub_id}: {e}"
                    continue

                record = {
                    'hub_id': hub_id,
                    'provider': row.get('Title', ''),
                    'spatial_coverage': row.get('Spatial Coverage', ''),
                    'is_part_of': row.get('ID', hub_id),
                    'member_of': row.get('Member Of', hub_id),
                    'title_source': row.get('Publisher', ''),
                    'default_bbox': row.get('Bounding Box', ''),
                    'raw_data': data
                }

                yield f"[ArcGIS] ✅ Fetched {hub_id} — {row.get('Title', 'No Title')}"
                yield record  # this sends the record downstream
                


    def parse(self, fetched_records):
        return fetched_records

    def flatten(self, parsed_data):
        rows = []
        for rec in parsed_data:
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
        return rows

    def build_dataframe(self, flat_data):
        return pd.DataFrame(flat_data)

    def derive_fields(self, df):
        df = (
            df.pipe(self.arcgis_parse_identifiers)
            .pipe(self.arcgis_format_titles)
            .pipe(self.arcgis_clean_descriptions)
            .pipe(self.arcgis_harvest_creators)
            .pipe(self.arcgis_build_keywords)
            .pipe(self.arcgis_harvest_dates)
            .pipe(self.arcgis_temporal_coverage)
            .pipe(self.arcgis_compute_bbox_column)
            .pipe(self.arcgis_build_distribution_columns)
            .pipe(self.arcgis_clean_creator_values)
            .pipe(self.arcgis_drop_rows_without_resource_class)
        )

        # Drop any remaining dict-based fields before deduplication/cleaning
        df = df.drop(columns=['creator_info', 'keywords_list', 'spatial', 'distributions'], errors='ignore')
        return df


    def add_defaults(self, df):
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
        df['Publication State'] = 'published'
        df['Is Part Of'] = df['is_part_of']
        df['Member Of'] = df['member_of']
        df['Spatial Coverage'] = df['spatial_coverage']
        df['Alternative Title'] = df['alternative_title']
        return df
    
    def add_provenance(self, df):
        today = time.strftime('%Y-%m-%d')
        df['Date Accessioned'] = today
        return df

    def clean(self, df):
        df = spatial_cleaning(df)
        df = super().clean(df)
        return df

    def validate(self, df):
        validation_pipeline(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        # Add back the secondary table logic here
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)

# --- ArcGIS-Specific Field Derive Functions ---

    def arcgis_harvest_identifier_and_id(self, identifier: str) -> tuple:
        parsed = urlparse(identifier)
        qs = parse_qs(parsed.query)
        if 'id' in qs:
            ds_id = qs['id'][0]
            cleaned = f"https://hub.arcgis.com/datasets/{ds_id}"
            return cleaned, ds_id
        return identifier, identifier

    def arcgis_parse_identifiers(self, df):
        ids = df['identifier_raw'].apply(self.arcgis_harvest_identifier_and_id)
        df[['Identifier', 'ID']] = pd.DataFrame(ids.tolist(), index=df.index)
        return df

    def arcgis_format_titles(self, df):
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


    def arcgis_clean_descriptions(self, df):
        def _clean(text):
            text = text.replace("{{default.description}}", "").replace("{{description}}", "")
            text = re.sub(r'[\n\r]+', ' ', text)
            text = re.sub(r'\s{2,}', ' ', text)
            return text.translate({
                8217: "'",  # RIGHT SINGLE QUOTATION MARK → apostrophe
                8220: '"',  # LEFT DOUBLE QUOTATION MARK → "
                8221: '"',  # RIGHT DOUBLE QUOTATION MARK → "
                160: "",    # NON-BREAKING SPACE → removed
                183: "",    # MIDDLE DOT → removed
                8226: "",   # BULLET → removed
                8211: '-',  # EN DASH → hyphen
                8203: ""    # ZERO WIDTH SPACE → removed
            })
        df['Description'] = df['description_raw'].apply(_clean)
        return df


    def arcgis_harvest_creator(self, info):
        if isinstance(info, dict):
            for v in info.values():
                return v.replace(u"\u2019", "'")
        return ''

    def arcgis_harvest_creators(self, df):
        df['Creator'] = df['creator_info'].apply(self.arcgis_harvest_creator)
        return df

    def arcgis_build_keywords(self, df):
        df['Keyword'] = df['keywords_list'].apply(lambda lst: '|'.join(lst).replace(' ', ''))
        return df

    def arcgis_harvest_dates(self, df):
        df['Date Issued'] = df['date_issued_raw'].str.split('T').str[0]
        df['Date Modified'] = df['date_modified_raw'].str.split('T').str[0]
        return df

    def arcgis_temporal_coverage(self, df):
        def _cov(r):
            # Case 1: Extract from {YYYY-YYYY} in title
            match = re.search(r"\{(.*?)\}", r['Title'])
            if match:
                tc = match.group(1)
                dr = tc if '-' in tc else f"{tc}-{tc}"
            else:
                # Always set Temporal Coverage from Date Modified
                modified = r.get('Date Modified', '')
                tc = f"Last modified {modified}" if modified else ''

                # Try to compute Date Range from Issued + Modified
                issued_year = r.get('Date Issued', '')[:4]
                modified_year = modified[:4]

                if issued_year.isdigit() and modified_year.isdigit():
                    dr = f"{issued_year}-{modified_year}"
                elif issued_year.isdigit():
                    dr = f"{issued_year}-{issued_year}"
                elif modified_year.isdigit():
                    dr = f"{modified_year}-{modified_year}"
                else:
                    dr = ''

            return pd.Series({'Temporal Coverage': tc, 'Date Range': dr})

        cov = df.apply(_cov, axis=1)
        return pd.concat([df, cov], axis=1)

    def arcgis_clean_creator_values(self, df):
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
        
    def arcgis_drop_rows_without_resource_class(self, df):
        df['Resource Class'] = df['Resource Class'].astype(str).replace(r'^\s*$', pd.NA, regex=True)
        df = df.dropna(subset=['Resource Class'])
        return df

    def arcgis_compute_bbox_column(self, df):
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


    def arcgis_build_distribution_columns(self, df):
        dist_df = df.apply(
            lambda r: pd.Series({
                k: str(v) if not isinstance(v, (list, pd.Series)) and pd.notna(v) else ''
                for k, v in self.arcgis_harvest_distribution_fields(r['distributions'], r['Title'], r['Description']).items()
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

    def arcgis_harvest_distribution_fields(self, distributions, title: str, description: str) -> dict:
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

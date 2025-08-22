import csv
import time
# import math
import os
import re
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range
# from utils.title_formatter import title_wizard


class ArcGISHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)

    def load_reference_data(self):
        super().load_reference_data()

    def fetch(self):
        hub_file = self.config.get("hub_list_csv")
        with open(hub_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                hub_id = row['ID']
                url = row['Endpoint URL']
                try:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    yield f"[ArcGIS] Error fetching {hub_id}: {e}"
                    continue

                row['raw_data'] = data
                yield f"[ArcGIS] Fetched {hub_id} — {row.get('Title', 'No Title')}"
                yield row


    # def parse(self, fetched_records):
    #     not needed

    def flatten(self, parsed_data):
        """
        De-nest raw_data['dataset'] into [{'hub': <hub_row>, 'ds': <dataset>}].
        No filtering or mapping here.
        """
        items = []
        for rec in parsed_data:
            if not isinstance(rec, dict):
                continue  # skip log strings yielded from fetch

            datasets = rec.get('raw_data', {}).get('dataset', []) or []
            if isinstance(datasets, dict):  # rare edge case
                datasets = [datasets]

            for ds in datasets:
                items.append({'hub': rec, 'ds': ds})
        return items
    
    def build_dataframe(self, flattened_items):
        df = pd.DataFrame(flattened_items)

        df = (
            df.pipe(self.arcgis_filter_rows)
            .pipe(self.arcgis_normalize_fields)
            .pipe(self.arcgis_extract_distributions)
            .pipe(self.arcgis_map_to_schema)
        )
        return df

    def derive_fields(self, df):
        df = (
            df.pipe(self.arcgis_parse_identifiers)
            .pipe(self.arcgis_clean_descriptions)
            .pipe(self.arcgis_temporal_coverage)
            .pipe(self.arcgis_format_date_ranges)
            .pipe(self.arcgis_compute_bbox_column)
            .pipe(self.arcgis_clean_creator_values)
            .pipe(self.arcgis_reformat_titles)
            .pipe(self.arcgis_set_resource_type)
        )

        # Drop any remaining dict-based fields before deduplication/cleaning
        df = df.drop(columns=['keywords_list', 'spatial', 'distributions'], errors='ignore')
        return df


    def add_defaults(self, df):
        df = super().add_defaults(df)

        df['Display Note'] = "Tip: Check “Visit Source” link for download options."
        df['Language'] = 'eng'
        df['Access Rights'] = 'Public'
        df['Resource Class'] = 'Web services'
        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Source Platform"] = "ArcGIS Hub"
        df["Accrual Method"] = "Scripted harvest"
        df["Harvest Workflow"] = "R01_arcgis"
        df["Supported Metadata Schema"] = "DCAT-US Schema v1.1"
        df["Endpoint Description"] = "DCAT API"
        df["Provenance Statement"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from "
                f"{row.get('Local Collection', ' ArcGIS Hub')} on {today}."
            ),
            axis=1,
        )

        # ---------- load hub list and evaluate indexing status ----------
        hub_path = self.config.get("hub_list_csv")

        if hub_path and os.path.exists(hub_path):
            hub_df = pd.read_csv(hub_path, dtype=str).fillna("")

            # Ensure required columns exist
            if "Status" not in hub_df.columns:
                hub_df["Status"] = ""

            # Convert to string for a reliable comparison
            indexed_ids = set(df["Is Part Of"].astype(str))

            hub_df["Status"] = hub_df["ID"].apply(
                lambda hub_id: "Indexed" if str(hub_id) in indexed_ids else "Not indexed"
            )

            hub_df["Date Accessioned"] = today


            # ---------- merge ----------
            df = pd.concat([df, hub_df], ignore_index=True)

            print(f"[ArcGIS] Updated Status for {len(hub_df)} hub records and "
                f"appended them to the harvested metadata dataframe.")
        else:
            print("[ArcGIS] hub_list_csv not found or unspecified.")

        return df



    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)

# --- ArcGIS-Specific Field Derive Functions --- #

    def arcgis_filter_rows(self, df):
        ALLOWED_TITLES = {'Shapefile'}
        ACCESS_PATTERNS = ['ImageServer']  # extend if needed, e.g., 'FeatureServer', 'MapServer'

        def is_valid(row):
            ds = row['ds']
            title = (ds.get('title') or '').strip()
            if not title or title.startswith('{{'):
                return False

            dists = ds.get('distribution', []) or []
            if not isinstance(dists, list):
                return False

            has_valid_title = any((dist.get('title') in ALLOWED_TITLES) for dist in dists)
            has_valid_url = any(
                any(pat in (dist.get('accessURL') or '') for pat in ACCESS_PATTERNS)
                for dist in dists
            )
            return has_valid_title or has_valid_url

        return df[df.apply(is_valid, axis=1)].reset_index(drop=True)
    
    def arcgis_normalize_fields(self, df):
        def get_creator(ds):
            pub = ds.get('publisher')
            if isinstance(pub, dict):
                return pub.get('name') or next(iter(pub.values()), '')
            return pub or ''

        df['title'] = df['ds'].apply(lambda d: (d.get('title') or '').strip())
        df['description'] = df['ds'].apply(lambda d: d.get('description', ''))
        df['creator'] = df['ds'].apply(get_creator)
        df['keywords'] = df['ds'].apply(lambda d: d.get('keyword', []) or [])
        df['issued'] = df['ds'].apply(lambda d: (d.get('issued', '') or '').split('T')[0])
        df['modified'] = df['ds'].apply(lambda d: (d.get('modified', '') or '').split('T')[0])
        df['license'] = df['ds'].apply(lambda d: d.get('license', ''))
        df['identifier_raw'] = df['ds'].apply(lambda d: d.get('identifier', ''))
        df['landingPage'] = df['ds'].apply(lambda d: d.get('landingPage', ''))
        df['spatial'] = df['ds'].apply(lambda d: d.get('spatial', ''))

        # hub context
        df['hub_id'] = df['hub'].apply(lambda h: h.get('ID', ''))
        df['hub_title'] = df['hub'].apply(lambda h: h.get('Title', ''))
        df['hub_creator'] = df['hub'].apply(lambda h: h.get('Creator', ''))
        df['hub_publisher'] = df['hub'].apply(lambda h: h.get('Publisher', ''))
        df['hub_endpoint'] = df['hub'].apply(lambda h: h.get('Endpoint URL', ''))
        df['hub_spatial_coverage'] = df['hub'].apply(lambda h: h.get('Spatial Coverage', ''))
        df['hub_bbox_default'] = df['hub'].apply(lambda h: h.get('Bounding Box', ''))
        df['hub_member_of'] = df['hub'].apply(lambda h: h.get('Member Of', ''))

        # keep distributions for next step
        df['distributions'] = df['ds'].apply(lambda d: d.get('distribution', []) or [])

        return df
    
    def arcgis_extract_distributions(self, df):
        def derive_dist_fields(dists):
            out = {
                'featureService': '',
                'mapService': '',
                'imageService': '',
                'tileService': '',
                'Format': '',
            }
            for dist in dists:
                title = dist.get('title', '')
                access_url = dist.get('accessURL', '') or ''
                if title == 'ArcGIS GeoService' and access_url:
                    if 'FeatureServer' in access_url:
                        out['featureService'] = access_url
                        out['Format'] = 'ArcGIS FeatureLayer'
                    elif 'MapServer' in access_url:
                        out['mapService'] = access_url
                        out['Format'] = 'ArcGIS DynamicMapLayer'
                    elif 'ImageServer' in access_url:
                        out['imageService'] = access_url
                        out['Format'] = 'ArcGIS ImageMapLayer'
                    elif 'TileServer' in access_url:
                        out['tileService'] = access_url
                        out['Format'] = 'ArcGIS TiledMapLayer'
            return pd.Series(out)

        dist_df = df['distributions'].apply(derive_dist_fields)
        # merge columns into df (aligned by index)
        for col in ['featureService', 'mapService', 'imageService', 'tileService', 'Format']:
            df[col] = dist_df[col]

        return df
    
    def arcgis_map_to_schema(self, df):
        df_out = pd.DataFrame()

        # Hub / input CSV
        df_out['Is Part Of'] = df['hub_id']
        df_out['Code'] = df['hub_id']            # keep as hub ID (matches your current pattern)
        df_out['Local Collection'] = df['hub_title']
        df_out['Publisher'] = df['hub_creator']  # your code previously used hub Creator here
        df_out['Endpoint URL'] = df['hub_endpoint']
        df_out['Spatial Coverage'] = df['hub_spatial_coverage']
        df_out['Bounding Box'] = df['hub_bbox_default']
        df_out['Member Of'] = df['hub_member_of']
        df_out['titlePlace'] = df['hub_publisher']

        # Dataset fields
        df_out['Alternative Title'] = df['title']
        df_out['Description'] = df['description']
        df_out['Creator'] = df['creator']
        df_out['Keyword'] = df['keywords'].apply(lambda lst: '|'.join(k.strip() for k in lst if isinstance(k, str)).replace(' ', ''))
        df_out['Date Issued'] = df['issued']
        df_out['Date Modified'] = df['modified']
        df_out['Rights'] = df['license']
        df_out['identifier_raw'] = df['identifier_raw']
        df_out['information'] = df['landingPage']

        # Keep raw spatial & default bbox for later bbox derivation
        df_out['spatial'] = df['spatial']

        # Distribution columns
        df_out['featureService'] = df['featureService']
        df_out['mapService'] = df['mapService']
        df_out['imageService'] = df['imageService']
        df_out['tileService'] = df['tileService']
        df_out['Format'] = df['Format']

        return df_out





    def arcgis_compute_bbox_column(self, df):
        """
        Populate 'Bounding Box' using 'spatial' if it has 4 comma-separated numbers
        and forms a non-degenerate box (xmin != xmax and ymin != ymax).
        Otherwise, use 'default_bbox'.
        """
        def _bbox(r):
            sp = r.get('spatial', None)
            fallback = r.get('default_bbox', '')

            def use_fallback():
                fb = '' if pd.isna(fallback) else str(fallback).strip()
                return fb

            if isinstance(sp, str):
                parts = [p.strip() for p in sp.split(',')]
                if len(parts) == 4:
                    try:
                        xmin, ymin, xmax, ymax = [float(p) for p in parts]

                        # Normalize if reversed
                        if xmin > xmax: xmin, xmax = xmax, xmin
                        if ymin > ymax: ymin, ymax = ymax, ymin

                        # Degenerate → line/point → use fallback
                        if xmin == xmax or ymin == ymax:
                            return use_fallback()

                        # Valid polygon bbox
                        return f"{xmin},{ymin},{xmax},{ymax}"
                    except ValueError:
                        pass

            # Not a valid 4-number bbox → use fallback
            return use_fallback()

        df['Bounding Box'] = df.apply(_bbox, axis=1)
        return df


    def arcgis_harvest_identifier_and_id(self, identifier: str) -> tuple:
        parsed = urlparse(identifier)
        qs = parse_qs(parsed.query)

        if 'id' in qs:
            ds_id = qs['id'][0]

            # Append sublayer number if present
            if 'sublayer' in qs:
                ds_id = f"{ds_id}_{qs['sublayer'][0]}"

            cleaned = f"https://hub.arcgis.com/datasets/{ds_id}"
            return cleaned, ds_id

        return identifier, identifier


    def arcgis_parse_identifiers(self, df):
        ids = df['identifier_raw'].apply(self.arcgis_harvest_identifier_and_id)
        df[['Identifier', 'ID']] = pd.DataFrame(ids.tolist(), index=df.index)
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
        df['Description'] = df['Description'].apply(_clean)
        return df

    def arcgis_temporal_coverage(self, df):
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def arcgis_format_date_ranges(self, df):
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df

    def arcgis_reformat_titles(self, df):
        """
        Updates the Title field by concatenating 'Alternative Title' and 'titlePlace',
        with the titlePlace in square brackets. Handles missing values gracefully.
        Example: "IDOT Waterway Ferries [Illinois]"
        """
        df['Title'] = df.apply(
            lambda row: f"{row['Alternative Title']} [{row['titlePlace']}]"
            if pd.notna(row['Alternative Title']) and pd.notna(row['titlePlace'])
            else row['Alternative Title'] if pd.notna(row['Alternative Title'])
            else f"[{row['titlePlace']}]" if pd.notna(row['titlePlace'])
            else "",
            axis=1
        )
        return df

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
    
    def arcgis_set_resource_type(self, df):
        """
        Assign values to 'Resource Type' based on keyword matches found in Title, Description, or Keyword.
        Existing values are preserved unless a new match is found.
        """
        keyword_map = {
            'lidar': 'LiDAR',
            'polygon': 'Polygon data',
            # Add more mappings here as needed
        }

        def match_keywords(row):
            combined_text = f"{row.get('Alternative Title', '')} {row.get('Description', '')} {row.get('Keyword', '')}".lower()
            for keyword, resource_type in keyword_map.items():
                if keyword in combined_text:
                    return resource_type
            return row.get('Resource Type', '')  # Keep existing value if no match

        df['Resource Type'] = df.apply(match_keywords, axis=1)
        return df








# def main():
#     """
#     Run ArcGIS harvestion standalone for local testing.
#     """
#     config_path = "config/arcgis.yaml"
#     schema_path = "schemas/geobtaa_schema.yaml"

#     with open(config_path, "r") as f:
#         config = yaml.safe_load(f)

#     Harvester = ArcGISHarvester(config, schema_path)
#     Harvester.harvest()

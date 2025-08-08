import csv
import time
import os
import re
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range
from utils.title_formatter import title_wizard


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
                    yield f"[ArcGIS] ❌ Error fetching {hub_id}: {e}"
                    continue

                row['raw_data'] = data
                yield f"[ArcGIS] ✅ Fetched {hub_id} — {row.get('Title', 'No Title')}"
                yield row


    # def parse(self, fetched_records):
    #     not needed

    def flatten(self, parsed_data):

        rows = []
        for rec in parsed_data:
            hub_id = rec['ID']
            for ds in rec.get('raw_data', {}).get('dataset', []):
                # --- safe title handling -----------------------------------------
                raw_title = ds.get('title')
                title = str(raw_title).strip() if raw_title not in (None, "") else "Untitled"

                # Skip empty titles or template placeholders like "{{name}}"
                if not title or title.startswith("{{"):
                    continue

                description = ds.get('description', '')
                distributions = ds.get('distribution', [])

                # Distribution filter: skip if no valid type
                allowed_titles = {'Shapefile'}
                allowed_access_patterns = ['ImageServer']

                has_valid_title = any(dist.get('title') in allowed_titles for dist in distributions)
                has_valid_url = any(
                    any(pattern in dist.get('accessURL', '') for pattern in allowed_access_patterns)
                    for dist in distributions
                )

                if not (has_valid_title or has_valid_url):
                    continue

                # Extract default from input CSV
                default_bbox = rec.get('Bounding Box', '')

                # Harvested value from metadata
                harvested_spatial = ds.get('spatial', '')

                # Fallback if spatial is blank, null, or contains invalid placeholders
                if isinstance(harvested_spatial, str) and (
                    not harvested_spatial.strip()
                    or harvested_spatial.strip().startswith("{{")
                ):
                    final_bbox = default_bbox
                else:
                    final_bbox = harvested_spatial

                # --- Flatten distribution fields ---
                dist_fields = {
                    'download': '',
                    'featureService': '',
                    'mapService': '',
                    'imageService': '',
                    'tileService': '',
                    'Format': '',
                }

                for dist in distributions:
                    dist_title = dist.get('title', '')
                    access_url = dist.get('accessURL', '')

                    if dist_title == 'Shapefile':
                        dist_fields['download'] = access_url
                        dist_fields['Format'] = 'Shapefile'
                    elif dist_title == 'ArcGIS GeoService' and access_url:
                        if 'FeatureServer' in access_url:
                            dist_fields['featureService'] = access_url
                        elif 'MapServer' in access_url:
                            dist_fields['mapService'] = access_url
                        elif 'ImageServer' in access_url:
                            dist_fields['imageService'] = access_url
                            dist_fields['Format'] = 'Imagery'
                        elif 'TileServer' in access_url:
                            dist_fields['tileService'] = access_url

                row = {
                    # Fields from input CSV
                    'Provider': rec.get('Title', ''),
                    'Spatial Coverage': rec.get('Spatial Coverage', ''),
                    'Is Part Of': rec.get('ID', ''),
                    'Code': rec.get('ID', ''),
                    'Member Of': rec.get('Member Of', ''),
                    'Publisher': rec.get('Publisher', ''),
                    'Endpoint URL': rec.get('Endpoint URL', ''),

                    # Fields to harvest from DCAT API
                    'Alternative Title': title,
                    'Description': description,
                    'Creator': next(iter(ds.get('publisher', {}).values()), ''),
                    'Keyword': '|'.join(ds.get('keyword', [])).replace(' ', ''),
                    'Date Issued': ds.get('issued', '').split('T')[0],
                    'Date Modified': ds.get('modified', '').split('T')[0],
                    'Rights': ds.get('license', ''),
                    'identifier_raw': ds.get('identifier', ''),
                    'information': ds.get('landingPage', ''),
                    'spatial': ds.get('spatial', {}),
                    'distributions': distributions,
                    'Bounding Box': final_bbox
                }

                row.update(dist_fields)
                rows.append(row)

        return rows


    def build_dataframe(self, flat_data):
        return pd.DataFrame(flat_data)

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

        df['Display Note'] = "This resource was automatically cataloged from the provider's ArcGIS Hub. In some cases, information shown here may be out-of-date. Click the 'Visit Source' button to search for items on the original provider's website."
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
        df["Was Generated By"] = "R01_arcgis"
        df["Supported Metadata Schema"] = "DCAT-US Schema v1.1"
        df["Endpoint Description"] = "DCAT API"
        df["Provenance Statement"] = df.apply(
            lambda row: (
                f"The metadata for this resource was harvested from "
                f"{row.get('Provider', 'an ArcGIS Hub')} on {today}."
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
                lambda hub_id: "Indexed" if str(hub_id) in indexed_ids else "Not Indexed"
            )

            hub_df["Date Accessioned"] = today


            # ---------- merge ----------
            df = pd.concat([df, hub_df], ignore_index=True)

            print(f"[ArcGIS] ✅ Updated Status for {len(hub_df)} hub records and "
                f"appended them to the metadata dataframe.")
        else:
            print("[ArcGIS] ⚠️ hub_list_csv not found or unspecified.")

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
        Updates the Title field using a formatting pipeline.
        """
        return title_wizard(df)


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

    def arcgis_compute_bbox_column(self, df):
        def _bbox(r):
            sp = r.get('spatial', None)          # this is the raw 'spatial' field from ArcGIS dataset JSON
            fallback = r.get('bbox_fallback', '')  # default bbox from your input CSV

            # Case 1: if sp is a valid bbox string, return it directly
            if isinstance(sp, str) and sp.count(',') == 3:
                return sp

            # Case 2: fallback to spreadsheet value if nothing else worked
            return fallback if pd.notnull(fallback) else ''

        df['Bounding Box'] = df.apply(_bbox, axis=1)
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

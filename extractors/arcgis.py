import csv
import time
import yaml
import requests
import pandas as pd
from utils.constants import FIELD_ORDER
from utils.constants import PRIMARY_FIELD_ORDER
from extractors.base import BaseExtractor
from utils.distribution_writer import build_secondary_table, load_distribution_types


class ArcGISExtractor(BaseExtractor):
    def __init__(self, config, schema):
        self.config = config
        self.schema = schema
        self.distribution_types = load_distribution_types()

    def fetch(self):
        hub_file = self.config["hub_list_csv"]
        all_records = []

        with open(hub_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                hub_id = row['ID']
                url = row['Identifier']
                provider = row.get('Title', "")
                spatial_coverage = row.get('Spatial Coverage', "")
                is_part_of = row.get('ID', "")
                member_of = row.get('Member Of', "")

                print(f"Fetching {hub_id} at {url}")
                try:
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    raw_data = response.json()
                except Exception as e:
                    print(f"Error fetching {hub_id}: {e}")
                    continue

                all_records.append({
                    "hub_id": hub_id,
                    "provider": provider,
                    "spatial_coverage": spatial_coverage,
                    "is_part_of": is_part_of,
                    "member_of": member_of,
                    "raw_data": raw_data
                })

        return all_records

    def normalize(self, fetched_records) -> list[dict]:
        normalized = []
        for record in fetched_records:
            raw_datasets = record["raw_data"].get("dataset", [])
            for ds in raw_datasets:
                dataset_id = ds.get("identifier", "")

                # Assign your default values from the hub row
                hub_code = record["hub_id"]
                provider = record.get("provider", "")
                url = ds.get("landingPage", "")
                spatial_coverage = record.get("spatial_coverage", "")
                is_part_of = record.get("is_part_of", hub_code)
                member_of = record.get("member_of", hub_code)
                date_accessioned = time.strftime('%Y-%m-%d')

                # Build base metadata record
                base_normalized = {
                    "Code": hub_code,
                    "ID": dataset_id,
                    "Title": ds.get("title", "Untitled"),
                    "Identifier": url,
                    "Provider": provider,
                    "Display Note": "This dataset was automatically cataloged from the provider's ArcGIS Hub. In some cases, information shown here may be incorrect or out-of-date. Click the 'Visit Source' button to search for items on the original provider's website.",
                    "Language": "eng",
                    "Access Rights": "Public",
                    "Accrual Method": "ArcGIS Hub",
                    "Date Accessioned": date_accessioned,
                    "Publication State": "published",
                    "Is Part Of": is_part_of,
                    "Member Of": member_of,
                    "Spatial Coverage": spatial_coverage,
                }

                # Initialize distribution fields to empty
                distribution_fields = {
                    "download": "",
                    "featureService": "",
                    "mapService": "",
                    "imageService": "",
                    "tileService": "",
                }

                # Extract distribution URLs if available
                distributions = ds.get("distribution", [])
                for dist in distributions:
                    dist_title = dist.get("title", "")
                    access_url = dist.get("accessURL", "")

                    # Shapefile download
                    if dist_title == "Shapefile":
                        distribution_fields["download"] = access_url

                    # ArcGIS GeoServices
                    if dist_title == "ArcGIS GeoService" and access_url:
                        if "FeatureServer" in access_url:
                            distribution_fields["featureService"] = access_url
                        elif "MapServer" in access_url:
                            distribution_fields["mapService"] = access_url
                        elif "ImageServer" in access_url:
                            distribution_fields["imageService"] = access_url
                        elif "TileServer" in access_url:
                            distribution_fields["tileService"] = access_url

                # Merge fields and append to normalized list
                base_normalized.update(distribution_fields)
                normalized.append(base_normalized)
# Convert list of dicts to DataFrame after building all records
        df = pd.DataFrame(normalized)

# Apply cleanup and reordering
        df = self.cleanup_and_reorder(df)

# Return final normalized records as list of dicts
        return df.to_dict(orient='records')
        # Reorder columns to include only primary fields
        primary_df = normalized_df.reindex(columns=[col for col in PRIMARY_FIELD_ORDER if col in normalized_df.columns])

        primary_df.to_csv(primary_out, index=False)


    def generate_secondary_table(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates the secondary distribution table aligned with distribution_types.yaml.
        """
        distribution_types = load_distribution_types()
        secondary_df = build_secondary_table(normalized_df, distribution_types)
        return secondary_df
        
    def cleanup_and_reorder(self, df):
        df = df.applymap(lambda x: x.strip('|- ') if isinstance(x, str) else x)
        cols = [c for c in FIELD_ORDER if c in df.columns]
        return df.reindex(columns=cols)
     
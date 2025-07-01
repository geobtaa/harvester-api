import csv
import yaml
import requests
import pandas as pd
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
                    "provider": row.get("Title", ""),
                    "raw_data": raw_data
                })

        return all_records

    def normalize(self, fetched_records) -> list[dict]:
        normalized = []
        for record in fetched_records:
            raw_datasets = record["raw_data"].get("dataset", [])
            for ds in raw_datasets:
                dataset_id = ds.get("identifier", "")
                base_normalized = {
                    "Code": record["hub_id"],
                    "ID": dataset_id,
                    "Title": ds.get("title", "Untitled"),
                }

                # Initialize distribution fields to empty
                distribution_fields = {
                    "download": "",
                    "featureService": "",
                    "mapService": "",
                    "imageService": "",
                    "tileService": "",
                }

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

                # Merge and append
                base_normalized.update(distribution_fields)
                normalized.append(base_normalized)

        return normalized

    def generate_secondary_table(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates the secondary distribution table aligned with distribution_types.yaml.
        """
        distribution_types = load_distribution_types()
        secondary_df = build_secondary_table(normalized_df, distribution_types)
        return secondary_df

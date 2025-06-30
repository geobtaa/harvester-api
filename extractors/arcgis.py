import csv
import requests
from extractors.base import BaseExtractor

class ArcGISExtractor(BaseExtractor):
    def fetch(self):
        hub_file = self.job_config["hub_list_csv"]
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
                normalized.append({
                    "Code": record["hub_id"],
                    "ID": dataset_id,
                    "Title": ds.get("title", "Untitled"),
                })
        return normalized


# Standard library
import csv
import os
import time

# Third-party
import pandas as pd

# Project-specific
from utils.field_order import FIELD_ORDER, PRIMARY_FIELD_ORDER


class BaseHarvester:
    """
    Base class for all Harvesters. Defines the shared interface
    and common utility methods like output writing and DataFrame cleanup.
    """

    def __init__(self, config: dict, schema: dict):
        self.config = config
        self.schema = schema

    def fetch(self):
        """
        Fetch raw data from the source.
        Must be implemented in child Harvesters.
        """
        raise NotImplementedError("fetch() must be implemented by the Harvester subclass.")

    def normalize(self, raw_data) -> tuple[list[dict], list[dict]]:
        """
        Normalize raw records into primary and secondary records.
        Must be implemented in child Harvesters.
        """
        raise NotImplementedError("normalize() must be implemented by the Harvester subclass.")

    def normalize_links(self, raw_links):
        """
        Optional stub for normalizing distribution links.
        """
        return raw_links

    def write_outputs(self, primary_records: list[dict], secondary_records: list[dict] = None) -> dict:
        """
        Write primary and optional secondary records to CSV outputs
        defined in the job configuration. Returns a dict of written file paths.
        """
        today = time.strftime("%Y-%m-%d")
        results = {}

        # Write primary CSV
        primary_out = self.config["output_primary_csv"]
        primary_filename = os.path.join("outputs", f"{today}_{os.path.basename(primary_out)}")
        primary_df = pd.DataFrame(primary_records)
        primary_df = primary_df.reindex(
            columns=[c for c in PRIMARY_FIELD_ORDER if c in primary_df.columns]
        )        
        primary_df.to_csv(primary_filename, index=False)
        results["primary_csv"] = primary_filename

        if secondary_records and self.config.get("output_distributions_csv"):
            # Determine fieldnames dynamically from your records
            fieldnames = list(secondary_records[0].keys())
            print(f"[DEBUG] Writing secondary CSV with fieldnames: {fieldnames}")

            secondary_filename = os.path.join(
                "outputs",
                f"{today}_{os.path.basename(self.config['output_distributions_csv'])}"
            )
            with open(secondary_filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(secondary_records)
            results["distributions_csv"] = secondary_filename


            return results

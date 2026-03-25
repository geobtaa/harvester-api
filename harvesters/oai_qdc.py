import csv
import time

import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table


class OaiQdcHarvester(BaseHarvester):
    """
    Scaffold for OAI-PMH feeds that expose qualified Dublin Core records.
    """

    EMPTY_PRIMARY_COLUMNS = [
        "ID",
        "Title",
        "Keyword",
        "Subject",
        "Date Range",
        "Bounding Box",
        "Access Rights",
        "Resource Class",
    ]

    def __init__(self, config):
        super().__init__(config)
        self.oai_base_url = self.config["oai_base_url"]
        self.metadata_prefix = self.config.get(
            "metadata_prefix",
            self.config.get("feed_type", "oai_qdc"),
        )
        self.sets_csv = self.config["sets_csv"]
        self.set_column = self.config.get("sets_csv_set_column", "set")
        self.set_title_column = self.config.get("sets_csv_title_column", "title")

    def fetch(self):
        sets = self.load_sets()
        print(
            f"[OAI_QDC] Loaded {len(sets)} set definitions from {self.sets_csv} "
            f"for {self.oai_base_url}"
        )
        return sets

    def build_dataframe(self, set_rows):
        if set_rows:
            print(
                f"[OAI_QDC] Crosswalk not implemented yet. Prepared scaffold for "
                f"{len(set_rows)} sets using metadataPrefix={self.metadata_prefix}."
            )
        else:
            print("[OAI_QDC] No set rows found. Returning an empty scaffold dataframe.")

        # Crosswalk to GeoBTAA fields will be added in a later step.
        return pd.DataFrame(columns=self.EMPTY_PRIMARY_COLUMNS)

    def add_provenance(self, df):
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = "OAI-PMH"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_oai_qdc"
        df["Endpoint Description"] = "OAI-PMH"
        df["Endpoint URL"] = self.oai_base_url
        df["Provenance"] = (
            "The metadata for this resource was last retrieved from the University "
            f"of Iowa Libraries OAI-PMH endpoint on {today}."
        )
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)

    def load_sets(self):
        sets = []
        with open(self.sets_csv, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                set_spec = str(row.get(self.set_column, "")).strip()
                set_title = str(row.get(self.set_title_column, "")).strip()
                if not set_spec:
                    continue
                sets.append(
                    {
                        "set_spec": set_spec,
                        "set_title": set_title,
                    }
                )
        return sets

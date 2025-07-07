import os
import time

import pandas as pd
from utils.field_order import PRIMARY_FIELD_ORDER  # adjust if you reorganize field orders

from utils.cleaning import basic_cleaning
from utils.validation import validation_pipeline
from utils.file_io import write_primary_and_distributions

class BaseHarvester:
    def __init__(self, config):
        # Initialize with config, schema paths, logging, etc.
        self.config = config

    def load_schema(self):
        # Load GeoBTAA or other target schema
        pass

    def fetch(self):
        # Download raw metadata
        pass

    def parse(self, raw_data):
        # Convert raw responses into structured dicts/lists
        pass

    def flatten(self, parsed_data):
        # (Optional) Expand nested structures to flat records
        return parsed_data

    def build_dataframe(self, parsed_or_flattened_data):
        # Map harvested fields to target schema + create DataFrame
        pass

    def derive_fields(self, df):
        # Generate complex fields
        return df

    def add_defaults(self, df):
        # Add schema-required default values
        return df
    
    def add_provenance(self, df):
        # (Optional) Add metadata about the harvest process
        return df

    def clean(self, df):
        """
        Perform data cleaning using the shared utility.
        Override in subclasses if source-specific cleaning is needed.
        """
        return basic_cleaning(df)

    def validate(self, df):
        """
        Perform data validation using the shared pipeline.
        Override in subclasses to customize validation logic.
        """
        validation_pipeline(df)  # might log or raise errors
        return df

    def write_outputs(self, primary_df: pd.DataFrame, distributions_df: pd.DataFrame = None) -> dict:
        """
        Write the primary and optional distributions DataFrames to CSVs in the outputs directory.
        Returns a dict of written file paths for logging or downstream use.

        Args:
            primary_df (pd.DataFrame): The main DataFrame of harvested records.
            distributions_df (pd.DataFrame, optional): DataFrame with distribution links, if applicable.
        """
        today = time.strftime("%Y-%m-%d")
        results = {}

        # Create outputs directory if it doesn't exist
        output_dir = "outputs"
        os.makedirs(output_dir, exist_ok=True)

        # Write primary CSV
        primary_out = self.config["output_primary_csv"]
        primary_filename = os.path.join(output_dir, f"{today}_{os.path.basename(primary_out)}")

        # Reorder columns to match schema field order
        primary_df = primary_df.reindex(
            columns=[col for col in PRIMARY_FIELD_ORDER if col in primary_df.columns]
        )
        primary_df.to_csv(primary_filename, index=False, encoding="utf-8")
        results["primary_csv"] = primary_filename

        # Write distributions CSV, if present
        if distributions_df is not None and self.config.get("output_distributions_csv"):
            distributions_out = self.config["output_distributions_csv"]
            distributions_filename = os.path.join(output_dir, f"{today}_{os.path.basename(distributions_out)}")

            distributions_df.to_csv(distributions_filename, index=False, encoding="utf-8")
            results["distributions_csv"] = distributions_filename

        return results


    def harvest_pipeline(self):
        """
        Orchestrate full workflow:
        load_schema → fetch → parse → flatten → build_dataframe →
        derive_fields → add_defaults → clean → validate → add_provenance → write_outputs
        """
        self.load_schema()
        raw = self.fetch()
        parsed = self.parse(raw)
        flat = self.flatten(parsed)
        df = self.build_dataframe(flat)

        df = (
            df.pipe(self.derive_fields)
              .pipe(self.add_defaults)
              .pipe(self.add_provenance)
              .pipe(self.clean)           # uses wrapper method → shared utility
              .pipe(lambda df: self.validate(df) or df)  # uses wrapper
        )

        self.write_outputs(df)            # uses wrapper

import os
import time
import pandas as pd

from utils.field_order import PRIMARY_FIELD_ORDER  
from utils.cleaner import basic_cleaning
from utils.validation import validation_pipeline

class BaseHarvester:
    def __init__(self, config):
        """
        Initialize harvester with a config dictionary. Should include paths to input files and output locations.
        """
        self.config = config

    def load_schema(self):
        """
        Optional: Load and store any schema you may need (e.g. field lists, data types, validation rules).
        Override this if your harvester needs to preload schema info.
        """
        return None

    def fetch(self):
        """
        REQUIRED: Retrieve raw metadata from a source (file, API, etc.).
        Should return either a list (of records) or a generator.
        """
        raise NotImplementedError("Subclasses must implement fetch()")


    def parse(self, raw_data):
        """
        Default passthrough. Override only if fetch() returns unstructured formats (HTML, raw strings, etc.).
        """
        return raw_data

    def flatten(self, parsed_data):
        """
        Default passthrough. Override if records are nested and need flattening to 1 row per record.
        """
        return parsed_data

    def build_dataframe(self, parsed_or_flattened_data):
        """
        REQUIRED: Convert a list of dicts into a Pandas DataFrame. Also responsible for mapping source fields to target schema.
        """
        raise NotImplementedError("Subclasses must implement build_dataframe()")


    def derive_fields(self, df):
        """
        Override to add specific transformations.
        """
        return df

    def add_defaults(self, df):
        """
        Optional: Add static default values required by schema (e.g. Provider name, Accrual Method).
        """
        return df
    
    def add_provenance(self, df):
        """
        Optional: Add harvest metadata (e.g. timestamp, harvester name) for internal tracking.
        """
        return df


    def clean(self, df):
        """
        Shared cleaning logic—removes extra pipes, trims whitespace, deduplicates, etc.
        Override in subclasses for source-specific cleaning.
        """
        return basic_cleaning(df)


    def validate(self, df):
        """
        Shared validation pipeline—logs errors or enforces required fields.
        Override to skip or customize rules.
        """
        validation_pipeline(df)
        return df


    def write_outputs(self, primary_df: pd.DataFrame, distributions_df: pd.DataFrame = None) -> dict:
        """
        Write the primary and optional distributions DataFrames to CSVs in the outputs directory.
        Returns a dict of written file paths for logging or downstream use.
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
        Main pipeline orchestrator. Runs all steps in order and writes output files.
        Subclasses can override specific steps but should not need to modify this method itself.
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
              .pipe(self.clean)
              .pipe(self.validate)
        )

        results = self.write_outputs(df)
        print(f"[Pipeline] Harvest complete: {results}")
        return results
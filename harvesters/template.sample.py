from utils.cleaning import basic_cleaning
from utils.validation import validation_pipeline
from utils.file_io import write_primary_and_distributions

class GenericHarvester(BaseHarvester):
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

    def write_outputs(self, df):
        """
        Write the final DataFrame using the shared output writer.
        Override if you need to change output formats or add steps.
        """
        output_dir = self.config.get('output_dir')
        write_primary_and_distributions(df, output_dir)

    def add_provenance(self, df):
        # (Optional) Add metadata about the harvest process
        return df

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
              .pipe(self.clean)           # uses wrapper method → shared utility
              .pipe(lambda df: self.validate(df) or df)  # uses wrapper
              .pipe(self.add_provenance)
        )

        self.write_outputs(df)            # uses wrapper

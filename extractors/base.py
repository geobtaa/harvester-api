class BaseExtractor:
    def __init__(self, job_config: dict, schema: dict):
        self.job_config = job_config
        self.schema = schema

    def fetch(self):
        """Fetch raw data from the source."""
        raise NotImplementedError

    def normalize(self, raw_data) -> list[dict]:
        """Normalize raw records into your local schema."""
        raise NotImplementedError


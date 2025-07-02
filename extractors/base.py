# Standard library
import os
import time
import re

# Project-specific
from utils.constants import FIELD_ORDER


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

    def cleanup_and_reorder(self, df):
        df = df.applymap(lambda x: x.strip('|- ') if isinstance(x, str) else x)
        cols = [c for c in FIELD_ORDER if c in df.columns]
        return df.reindex(columns=cols)
        
        
    # might not be currently used
    def normalize_links(self, raw_links):
        return raw_links

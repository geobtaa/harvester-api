import yaml
import pandas as pd
import os

def load_local_schema(schema_path="schemas/geobtaa_schema.yaml"):
    """Load the local metadata schema from YAML."""
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)
    return schema
    

def load_local_schema(schema_path="schemas/geobtaa_schema.yaml"):
    with open(schema_path, "r", encoding="utf-8") as f:
        import yaml
        return yaml.safe_load(f)

def write_csv(records: list[dict], output_path: str):
    """
    Write normalized records to CSV using Pandas.
    """
    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")

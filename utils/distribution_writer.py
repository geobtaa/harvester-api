import yaml
import pandas as pd

def load_distribution_types(yaml_path="schemas/distribution_types.yaml"):
    """
    Loads distribution types configuration from a YAML file.

    Returns a list of distribution type dictionaries with keys:
    - key: reference type (e.g., 'download', 'wms')
    - name: human-readable name
    - reference_uri: standard reference URI
    - variables: list of column names to match in the DataFrame
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config.get("distribution_types", [])

def build_secondary_table(df, distribution_types, id_field="ID"):
    """
    Returns a DataFrame with standardized columns:
    friendlier_id, reference_type, distribution_url, label.
    Matches each 'variable' field in the dataframe to its distribution type from the YAML config.
    """
    rows = []
    for dist in distribution_types:
        key = dist["key"]
        for variable in dist.get("variables", []):
            if variable in df.columns:
                for _, row in df.iterrows():
                    url = row.get(variable)
                    if pd.notna(url) and str(url).strip():
                        rows.append({
                            "friendlier_id": row[id_field],
                            "reference_type": key,
                            "distribution_url": url,
                            "label": row.get("Format", "") if key == "download" else ""
                        })
    return pd.DataFrame(rows)

def generate_secondary_table(normalized_df, distribution_types):
    """
    Generates the secondary distribution table from a normalized DataFrame
    using the given distribution_types configuration.
    """
    return build_secondary_table(normalized_df, distribution_types)


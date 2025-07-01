import yaml
import pandas as pd

def load_distribution_types(yaml_path="schemas/distribution_types.yaml"):
    """
    Loads distribution types config from YAML and returns the list.
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


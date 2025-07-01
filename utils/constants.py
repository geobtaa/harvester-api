# utils/constants.py

import yaml
import os

def load_field_order_from_schema(schema_path="schemas/geobtaa_schema.yaml") -> list:
    """
    Loads your canonical field order from geobtaa_schema.yaml
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    # Always include primary keys first
    field_order = schema.get("primaryKey", [])
    # Then append each defined field in order
    for field in schema.get("fields", []):
        name = field.get("name")
        if name and name not in field_order:
            field_order.append(name)
    return field_order

# You can expose it as a constant so other modules can just import FIELD_ORDER:
FIELD_ORDER = load_field_order_from_schema()

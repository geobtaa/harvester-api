import yaml

def load_field_order_from_schemas(
    primary_schema_path="schemas/geobtaa_schema.yaml",
    dist_schema_path="schemas/distribution_types.yaml",
) -> list:
    """
    Loads canonical field order from both the main geobtaa_schema.yaml and distribution_types.yaml.
    Returns a combined FIELD_ORDER list.
    """
    # Load primary schema
    with open(primary_schema_path, "r", encoding="utf-8") as f:
        primary_schema = yaml.safe_load(f)

    field_order = primary_schema.get("primaryKey", []) + [
        f["name"] for f in primary_schema.get("fields", []) if "name" in f
    ]

    # Load distribution schema
    with open(dist_schema_path, "r", encoding="utf-8") as f:
        dist_schema = yaml.safe_load(f)

    link_vars = []
    for dist in dist_schema.get("distribution_types", []):
        link_vars.extend(dist.get("variables", []))

    # Combine: primary fields first, then harvested link fields
    return field_order + link_vars

FIELD_ORDER = load_field_order_from_schemas()

def load_primary_field_order(schema_path="schemas/geobtaa_schema.yaml") -> list:
    """
    Loads only the canonical metadata field order (excludes distribution fields).
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    field_order = schema.get("primaryKey", [])
    for field in schema.get("fields", []):
        name = field.get("name")
        if name and name not in field_order:
            field_order.append(name)
    return field_order

PRIMARY_FIELD_ORDER = load_primary_field_order()


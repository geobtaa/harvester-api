import yaml

def load_field_order_from_schemas(
    primary_schema_path="schemas/geobtaa_schema.yaml",
    dist_schema_path="schemas/distribution_types.yaml",
) -> list:
    """
    Loads canonical field order from both the main geobtaa_schema.yaml and distribution_types.yaml,
    ensuring unique fields in their declared order.
    Returns a combined FIELD_ORDER list.
    """
    # Load primary schema
    with open(primary_schema_path, "r", encoding="utf-8") as f:
        primary_schema = yaml.safe_load(f)

    field_names = []
    seen = set()

    # Add primary keys first, avoiding duplicates
    for pk in primary_schema.get("primaryKey", []):
        if pk not in seen:
            field_names.append(pk)
            seen.add(pk)

    # Add additional fields, skipping ones already included
    for f in primary_schema.get("fields", []):
        name = f.get("name")
        if name and name not in seen:
            field_names.append(name)
            seen.add(name)

    # Load distribution schema and add link variables, skipping duplicates
    with open(dist_schema_path, "r", encoding="utf-8") as f:
        dist_schema = yaml.safe_load(f)

    link_vars = []
    for dist in dist_schema.get("distribution_types", []):
        for var in dist.get("variables", []):
            if var not in seen:
                link_vars.append(var)
                seen.add(var)

    return field_names + link_vars

FIELD_ORDER = load_field_order_from_schemas()

def load_primary_field_order(schema_path="schemas/geobtaa_schema.yaml") -> list:
    """
    Loads only the canonical primary metadata field order (excludes distribution fields).
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    field_order = []
    seen = set()

    for pk in schema.get("primaryKey", []):
        if pk not in seen:
            field_order.append(pk)
            seen.add(pk)

    for field in schema.get("fields", []):
        name = field.get("name")
        if name and name not in seen:
            field_order.append(name)
            seen.add(name)

    return field_order


PRIMARY_FIELD_ORDER = load_primary_field_order()

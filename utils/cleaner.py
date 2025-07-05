import pandas as pd
import numpy as np
from utils.field_order import FIELD_ORDER

def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform basic DataFrame cleaning:
    - Strip unwanted characters from string cells
    - Remove duplicate columns
    - Remove duplicate rows
    - Reset index
    - Reorder columns to FIELD_ORDER if defined
    """
    # Strip unwanted characters
    df = df.map(lambda x: x.strip('|- ') if isinstance(x, str) else x)

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Remove completely duplicate rows
    df = df.drop_duplicates()

    # Drop exact duplicate rows
    before_dedup = len(df)
    df = df.drop_duplicates()
    after_dedup = len(df)
    if after_dedup < before_dedup:
        print(f"[CLEAN] Dropped {before_dedup - after_dedup} exact duplicate rows.")

    # Drop rows with duplicate IDs
    if 'ID' in df.columns:
        before_id_dedup = len(df)
        df = df.drop_duplicates(subset=['ID'], keep='first')
        after_id_dedup = len(df)
        if after_id_dedup < before_id_dedup:
            print(f"[CLEAN] Dropped {before_id_dedup - after_id_dedup} rows with duplicate IDs.")
    else:
        print("[CLEAN] Warning: 'ID' column not found; skipping duplicate ID removal.")


    # Reset index
    df = df.reset_index(drop=True)

    # Reorder columns based on FIELD_ORDER
    cols = [c for c in FIELD_ORDER if c in df.columns]
    df = df.reindex(columns=cols)

    print(f"[CLEAN] Basic cleaning complete: {len(df)} unique rows, {len(df.columns)} unique columns.")
    return df


def round_coordinates(df):
    """
    Rounds coordinates in the 'Bounding Box' field to 3 decimal places.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            coords = x.split(',')
            rounded = [f"{float(c):.3f}" for c in coords]
            return ','.join(rounded)
        except Exception as e:
            print(f"[CLEAN] Skipped rounding invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def correct_bounding_box(df):
    """
    Ensures 'Bounding Box' coordinates are in proper order:
    west <= east, south <= north.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            west, south, east, north = map(float, x.split(','))
            corrected = False

            if east < west:
                west, east = min(west, east), max(west, east)
                corrected = True

            if north < south:
                south, north = min(south, north), max(south, north)
                corrected = True

            corrected_bbox = f"{west:.3f},{south:.3f},{east:.3f},{north:.3f}"
            if corrected:
                print(f"[CLEAN] Corrected bbox order: {corrected_bbox}")
            return corrected_bbox
        except Exception as e:
            print(f"[CLEAN] Skipped correcting invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def clean_bounding_box(df):
    """
    Adjusts extreme or degenerate 'Bounding Box' coordinates.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            west, south, east, north = map(float, x.split(','))

            # Clamp extreme longitudes and latitudes
            if abs(west) >= 180: west = np.sign(west) * 179.999
            if abs(east) >= 180: east = np.sign(east) * 179.999
            if north >= 90: north = 89.999
            if south <= -90: south = -89.999

            # Expand zero-width boxes slightly
            if abs(east - west) < 0.0001:
                east += 0.0001
            if abs(north - south) < 0.0001:
                north += 0.0001

            cleaned_bbox = f"{west:.3f},{south:.3f},{east:.3f},{north:.3f}"
            return cleaned_bbox
        except Exception as e:
            print(f"[CLEAN] Skipped cleaning invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def spatial_cleaning(df):
    """
    Apply all spatial cleaning steps to the DataFrame.
    """
    df = round_coordinates(df)
    df = correct_bounding_box(df)
    df = clean_bounding_box(df)
    return df

#####################Validation##################################

def validate_required_columns(df, required_columns=None):
    """
    Ensure required columns are present in the DataFrame.
    Raises ValueError if any required columns are missing.
    """
    if required_columns is None:
        required_columns = ['ID', 'Title', 'Access Rights', 'Resource Class']

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"[VALIDATION] Missing required columns: {', '.join(missing)}")
    print(f"[VALIDATION] All required columns present: {', '.join(required_columns)}")
    return df

def validate_access_rights(df):
    """
    Validates that 'Access Rights' values are within allowed list.
    Raises ValueError if any invalid values are found.
    """
    valid_values = {'Public', 'Restricted'}
    invalid = df.loc[~df['Access Rights'].isin(valid_values), 'Access Rights'].unique()
    if len(invalid) > 0:
        raise ValueError(f"[VALIDATION] Invalid Access Rights values: {invalid}")
    print("[VALIDATION] All Access Rights values are valid.")
    return df

def validate_resource_class(df):
    """
    Validates that 'Resource Class' contains at least one acceptable class.
    Raises ValueError if rows have Resource Class empty or invalid.
    """
    valid_classes = {'Collections', 'Datasets', 'Imagery', 'Maps', 'Web services', 'Websites', 'Series','Other'}
    def is_valid(cell):
        if pd.isnull(cell): return False
        classes = [c.strip() for c in str(cell).split('|')]
        return any(c in valid_classes for c in classes)
    
    invalid_rows = df[~df['Resource Class'].apply(is_valid)]
    if not invalid_rows.empty:
        raise ValueError(f"[VALIDATION] Found rows with invalid Resource Class values.")
    print("[VALIDATION] All Resource Class values are valid.")
    return df

def validate_bounding_box(df):
    """
    Checks Bounding Box column for numeric coordinate validity.
    Raises ValueError if coordinates fall outside plausible ranges.
    """
    def check_bbox(x):
        try:
            coords = list(map(float, x.split(',')))
            return (
                -180 <= coords[0] <= 180 and
                -90 <= coords[1] <= 90 and
                -180 <= coords[2] <= 180 and
                -90 <= coords[3] <= 90
            )
        except Exception:
            return False

    invalid_bboxes = df.loc[~df['Bounding Box'].apply(lambda x: check_bbox(x) if isinstance(x, str) else False)]
    if not invalid_bboxes.empty:
        raise ValueError(f"[VALIDATION] Found rows with invalid Bounding Boxes.")
    print("[VALIDATION] All Bounding Box coordinates are within valid ranges.")
    return df

def validation_pipeline(df):
    """
    Run all validations on the DataFrame.
    """
    df = validate_required_columns(df)
    df = validate_access_rights(df)
    df = validate_resource_class(df)
    df = validate_bounding_box(df)
    return df



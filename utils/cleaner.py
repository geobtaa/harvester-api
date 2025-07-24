import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from utils.field_order import FIELD_ORDER

######### Basic Cleaning #############

def deduplicate_rows_and_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate columns and deduplicate rows based on the 'ID' column, if present.
    Also resets the index.
    """
    # Drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Deduplicate based on ID if present
    if 'ID' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset='ID', keep='first')
        after = len(df)
        if after < before:
            print(f"[CLEAN] Dropped {before - after} rows with duplicate IDs.")
    else:
        print("[CLEAN] Warning: 'ID' column not found; skipping ID-based deduplication.")

    # Reset index
    df = df.reset_index(drop=True)

    return df

def reorder_columns(df: pd.DataFrame, field_order: list = FIELD_ORDER) -> pd.DataFrame:
    """
    Reorder columns based on predefined FIELD_ORDER.
    """
    cols = [c for c in field_order if c in df.columns]
    return df.reindex(columns=cols)

def strip_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip HTML and unwanted characters from all string fields in the DataFrame.
    Applies BeautifulSoup cleanup and trims '|', '-', and whitespace.
    """
    def clean_cell(cell):
        if isinstance(cell, str):
            # Remove HTML tags
            text = BeautifulSoup(cell, "html.parser").get_text()
            # Strip unwanted characters
            return text.strip().strip('|')
        return cell

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].map(clean_cell)

    return df


def create_date_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the 'Date Range' field:
    - Fixes reversed ranges (e.g., 2024-2020 → 2020-2024)
    - Clears the field if it contains non-integer values
    """
    def _clean(row):
        x = row.get('Date Range', '')
        if pd.isnull(x) or x == '':
            return x

        date_ranges = str(x).split('|')
        for i in range(len(date_ranges)):
            years = date_ranges[i].split('-')

            if all(y.isdigit() for y in years):
                if len(years) == 2 and int(years[0]) > int(years[1]):
                    years = sorted(years)
                    date_ranges[i] = '-'.join(years)
            else:
                return ''  # Clear non-integer values immediately

        return '|'.join(date_ranges)

    df['Date Range'] = df.apply(_clean, axis=1)
    return df

def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run core cleaning steps on the DataFrame:
    - Remove duplicate rows and columns
    - Strip HTML and unwanted characters from all text fields
    - Create standardized date ranges
    - Reorder columns based on FIELD_ORDER
    """
    df = (
        df.pipe(deduplicate_rows_and_columns)
          .pipe(strip_text_fields)
          .pipe(create_date_ranges)
          .pipe(reorder_columns)
    )
    print(f"[CLEAN] Basic cleaning complete: {len(df)} rows, {len(df.columns)} columns.")
    return df



######## SPATIAL FIELDS CLEANING ##############


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



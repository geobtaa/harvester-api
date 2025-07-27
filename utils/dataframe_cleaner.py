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

def dataframe_cleaning(df: pd.DataFrame) -> pd.DataFrame:
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
    print(f"[CLEAN] Dataframe cleaning complete: {len(df)} rows, {len(df.columns)} columns.")
    return df


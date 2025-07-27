# Standard library
import re
import json

# Third-party
import pandas as pd
from bs4 import BeautifulSoup

# Project-specific
from utils.distribution_writer import generate_secondary_table
from harvesters.base import BaseHarvester
from utils.cleaner import spatial_cleaning
from utils.validation import validation_pipeline

class PasdaHarvester(BaseHarvester):

    def __init__(self, config):
        super().__init__(config)
        self.counties_in_pennsylvania = []
        self.cities_in_pennsylvania = []
        self.spatial_data = pd.DataFrame()

    def load_reference_data(self):
        super().load_reference_data()

        # Load JSON of county names
        locations_path = self.config.get("locations_json", "data/locations.json")
        with open(locations_path, "r", encoding="utf-8") as f:
            locations = json.load(f)

        self.counties_in_pennsylvania = locations["counties_in_pennsylvania"]
        self.cities_in_pennsylvania = locations["cities_in_pennsylvania"]

        # Load spatial CSV with bbox and geonames
        spatial_csv_path = "data/spatial_counties.csv"
        self.spatial_data = pd.read_csv(spatial_csv_path)

    def fetch(self):
        html_path = self.config["input_html"]
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()

    def parse(self, raw_html):
        soup = BeautifulSoup(raw_html, 'html.parser')
        rows = []

        for entry in soup.select('td > h3 > a[href^="DataSummary.aspx?dataset="]'):
            try:
                # Must have visible title text
                title = entry.get_text(strip=True)
                if not title:
                    continue

                # Look ahead for related tags
                row = entry.find_parent("tr")
                if not row:
                    continue

                publisher_tag = entry.find_next("td")
                date_tag = entry.find_previous("td").find_previous("td")
                desc_tag = entry.find_next("span", id=lambda x: x and x.startswith('DataGrid1_Label3_'))

                # Basic content extraction
                publisher = publisher_tag.get_text(strip=True) if publisher_tag else ""
                date_issued = date_tag.get_text(strip=True) if date_tag else ""
                description = desc_tag.get_text(strip=True) if desc_tag else ""

                # Skip records with no description or publisher (optional)
                if not description or not publisher:
                    continue

                # Validate Metadata and Download links
                meta_tag = row.find('a', string='Metadata')
                if not meta_tag or not meta_tag.get('href'):
                    continue

                metadata_link = f"https://www.pasda.psu.edu/uci/{meta_tag['href']}"
                dl_tag = row.find('a', string='Download')
                download_link = f"https://www.pasda.psu.edu/uci/{dl_tag['href']}" if dl_tag and dl_tag.get('href') else ''

                landing_page = f"https://www.pasda.psu.edu/uci/{entry['href']}"
                identifier = 'pasda-' + landing_page.rsplit('=', 1)[-1]
                if not identifier:
                    continue

                # Build the row
                rows.append({
                    'Creator': publisher,
                    'Date Issued': date_issued,
                    'Alternative Title': title,
                    'Description': description,
                    'html': metadata_link,
                    'download': download_link,
                    'information': landing_page,
                    'ID': identifier
                })

            except Exception as e:
                print(f"[PASDA] Skipping entry due to error: {e}")
                continue

        print(f"[PASDA] Parsed {len(rows)} valid records from HTML")
        return pd.DataFrame(rows)



    def flatten(self, parsed_data):
        """
        Flattening not needed; returns parsed_data unchanged.
        """
        return parsed_data

    def build_dataframe(self, parsed_or_flattened_data):
        return pd.DataFrame(parsed_or_flattened_data)

    def derive_fields(self, df):
        """
        Apply PASDA-specific transformations in order.
        """
        return (
            df.pipe(self.pasda_drop_incomplete)
              .pipe(self.pasda_drop_federal)
              .pipe(self.pasda_transform_titles)
              .pipe(self.pasda_format_date_ranges)
              .pipe(self.pasda_append_spatial_fields)
        )
    
    def add_defaults(self, df):
        df = super().add_defaults(df)
        df['Code'] = '08a-01'
        df['Provider'] = 'Pennsylvania Spatial Data Access (PASDA)'
        df['Language'] = 'eng'
        df['Access Rights'] = 'Public'
        df['Is Part Of'] = '08a-01'
        df['Member Of'] = 'ba5cc745-21c5-4ae9-954b-72dd8db6815a'
        df['Format'] = 'File'
        df['Resource Class'] = 'Datasets'
        df['Access Rights'] = 'Public'

        return df
    
    def add_provenance(self, df):
        df = super().add_provenance(df)
        df['Accrual Method'] = 'HTML'
        return df
    
    def clean(self, df):
        df = spatial_cleaning(df)
        df = super().clean(df)
        return df

    def validate(self, df):
        validation_pipeline(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)


    # ────── PASDA Custom Methods ──────

    def pasda_drop_incomplete(self, df):
        """
        Drops rows missing required fields like 'ID' or 'Alternative Title'.
        Logs how many were dropped.
        """
        before = len(df)
        df = df[df['ID'].notna() & df['Alternative Title'].notna()].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f"[PASDA] Dropped {dropped} records missing 'ID' or 'Alternative Title'.")
        return df

    def pasda_drop_federal(self, df):
        federal = [
            "United States Army Corps of Engineers USACE", "U S Geological Survey", "U S Fish and Wildlife Service",
            "U S Environmental Protection Agency", "U S Department of Justice", "U S Department of Commerce",
            "U S Department of Agriculture", "U S Census Bureau", "National Weather Service NOAA NWS",
            "National Renewable Energy Laboratory NREL", "National Park Service", "National Geodetic Survey",
            "National Aeronautics and Space Administration NASA", "Federal Emergency Management Agency"
        ]

        if df.empty or 'Creator' not in df.columns:
            print("[PASDA] Skipping federal filter: 'Creator' column not found or DataFrame is empty.")
            return df

        return df[~df['Creator'].isin(federal)].reset_index(drop=True)


    def pasda_format_date_ranges(self, df):
        if df.empty or 'Date Issued' not in df.columns:
            print("[PASDA] Skipping date range formatting: 'Date Issued' column not found or DataFrame is empty.")
            return df

        def make_range(s):
            years = re.findall(r"(\d{4})", s)
            return f"{years[0]}-{years[-1]}" if years else s

        df['Date Range'] = df['Date Issued'].apply(make_range)
        return df


    def pasda_transform_titles(self, df):
        if df.empty or 'Alternative Title' not in df.columns or 'Date Issued' not in df.columns:
            print("[PASDA] Skipping title transformation: required columns not found or DataFrame is empty.")
            df['Title'] = ''
            df['Spatial Coverage'] = ''
            return df

        def safe_transform(row):
            try:
                title, coverage = self.pasda_transform_title(row)
                return pd.Series([title, coverage])
            except Exception as e:
                print(f"[PASDA] Title transform error: {e}")
                return pd.Series(['', ''])

        df[['Title', 'Spatial Coverage']] = df.apply(safe_transform, axis=1)
        return df


    def pasda_transform_title(self, row):
        alt_title = row.get('Alternative Title', '')
        coverage = ""

        # Replace county names
        for county in self.counties_in_pennsylvania:
            if re.search(f"{county} County", alt_title, re.I):
                alt_title = re.sub(f"{county} County", f"[Pennsylvania--{county} County]", alt_title, flags=re.I, count=1)
                coverage = f"{county} County"
                break
        else:
            for city in self.cities_in_pennsylvania:
                if re.search(rf"\b{city}\b", alt_title, re.I):
                    alt_title = re.sub(rf"\b{city}\b", f"[Pennsylvania--{city}]", alt_title, flags=re.I, count=1)
                    coverage = city
                    break
            else:
                alt_title = re.sub(r"\b(PA|Pennsylvania)\b", "[Pennsylvania]", alt_title, flags=re.I, count=1)
                coverage = "Pennsylvania"

        bracket_content = re.findall(r'\[(.*?)\]', alt_title)
        if bracket_content:
            alt_title = re.sub(r'\[.*?\]', '', alt_title).strip()
            alt_title = f"{alt_title} [{bracket_content[0]}]"

        alt_title = re.sub(r"For\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The City Of\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"^\s*-\s*|\s*-\s*(?=\[)", "", alt_title)

        if alt_title:
            alt_title = alt_title[0].capitalize() + alt_title[1:]

        alt_title += f" {{{row.get('Date Issued', '')}}}"

        return alt_title, coverage


    def pasda_append_spatial_fields(self, df):
        # Ensure the 'Spatial Coverage' column exists — create if missing
        if 'Spatial Coverage' not in df.columns:
            print("[PASDA] 'Spatial Coverage' column not found. Creating blank column.")
            df['Spatial Coverage'] = ''

        # Perform the merge even if many values are blank or unmatched
        if 'County' not in self.spatial_data.columns:
            print("[PASDA] Spatial data missing 'County' column. Skipping spatial join.")
            merged_df = df.copy()
        else:
            merged_df = pd.merge(
                df, self.spatial_data, left_on='Spatial Coverage', right_on='County', how='left'
            )
            print("[PASDA] Merged harvested records with spatial data on 'Spatial Coverage'.")

        # Default values for any missing spatial metadata
        default_values = {
            'Bounding Box': "-80.52,39.72,-74.69,42.27",
            'Geometry': (
                "MultiPolygon(((-75.6 39.8, -75.8 39.7, -80.5 39.7, -80.5 42.3, "
                "-79.8 42.5, -79.8 42, -75.3 42, -75.1 41.8, -75 41.5, -74.7 41.4, "
                "-75.1 41, -75.1 40.9, -75.2 40.7, -74.7 40.2, -75.1 39.9, -75.6 39.8)))"
            ),
            'GeoNames': "http://sws.geonames.org/6254927"
        }

        for column, default in default_values.items():
            if column in merged_df.columns:
                missing_count = merged_df[column].isna().sum()
                merged_df[column] = merged_df[column].fillna(default)
            else:
                missing_count = len(merged_df)
                merged_df[column] = default
            print(f"[PASDA] Set default for missing '{column}' values ({missing_count} rows updated).")

        return merged_df


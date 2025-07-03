# Standard library
import os
import time
import re
import json
import yaml

# Third-party
import pandas as pd
from bs4 import BeautifulSoup

# Project-specific
from utils.distribution_writer import load_distribution_types, generate_secondary_table
from harvesters.base import BaseHarvester
from utils.cleaner import basic_cleaning, spatial_cleaning, validation_pipeline


class PasdaHarvester(BaseHarvester):
    """
    Harvester for PASDA HTML datasets. Fetches local or remote HTML,
    parses dataset entries, normalizes metadata, and writes primary
    and secondary CSV outputs.
    """

    def __init__(self, config, schema):
        super().__init__(config, schema)

        self.distribution_types = load_distribution_types()

        # Load supporting location data (for place name normalization)
        default_locations_path = os.path.join("data", "locations.json")
        locations_path = self.config.get("locations_json", default_locations_path)
        

        try:
            with open(locations_path, "r", encoding="utf-8") as f:
                locations = json.load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Locations JSON not found: {locations_path}") from e

        if "counties_in_pennsylvania" not in locations or "cities_in_pennsylvania" not in locations:
            raise ValueError(f"Missing expected keys in locations JSON: {locations_path}")

        self.counties_in_pennsylvania = locations["counties_in_pennsylvania"]
        self.cities_in_pennsylvania = locations["cities_in_pennsylvania"]


        spatial_csv_path = os.path.join("data", "spatial_counties.csv")
        try:
            self.spatial_data = pd.read_csv(spatial_csv_path)
            print(f"[PASDA] Loaded spatial counties CSV with {len(self.spatial_data)} rows.")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Spatial CSV not found: {spatial_csv_path}") from e

    def fetch(self):
        """
        Read PASDA HTML from a local file defined in the job config.
        """
        html_path = self.config["input_html"]
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()

    def normalize(self, raw_html):
        """
        Parse PASDA HTML into records, clean, and transform titles.
        Returns primary and secondary records as lists of dicts.
        """
        df = self.parse_pasda_html(raw_html)
        df = (
            df.pipe(self.drop_federal)
              .pipe(self.format_date_ranges)
              .pipe(self.add_default_values)
              .pipe(self.transform_titles)
              .pipe(self.append_spatial_fields)
              .pipe(spatial_cleaning)
              .pipe(basic_cleaning)
              .pipe(validation_pipeline)
        )
        
        # df = self.append_spatial_fields(df)
        primary_records = df.to_dict(orient='records')
        print(f"[DEBUG] Generating secondary table from {len(primary_records)} primary records")
        secondary_df = generate_secondary_table(pd.DataFrame(primary_records), self.distribution_types)
        print(f"[DEBUG] Secondary dataframe rows: {len(secondary_df)}")
        return primary_records, secondary_df.to_dict(orient='records')


    def harvest(self):
        """
        Full workflow: fetch data, normalize it, and write outputs.
        Returns dict of generated file paths.
        """
        print("[PASDA] Fetching HTML...")
        raw_html = self.fetch()
        print("[PASDA] Normalizing data...")
        primary, secondary = self.normalize(raw_html)
        results = self.write_outputs(primary, secondary)
        print(f"[PASDA] Completed harvest: {results}")
        return results

    def parse_pasda_html(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        rows = []
        for entry in soup.select('td > h3 > a[href^="DataSummary.aspx?dataset="]'):
            publisher = entry.find_next("td").get_text(strip=True)
            date_issued = entry.find_previous("td").find_previous("td").get_text(strip=True)
            title = entry.get_text(strip=True)
            description = entry.find_next("span", id=lambda x: x and x.startswith('DataGrid1_Label3_')).get_text(strip=True)
            metadata_href = entry.parent.parent.find('a', string='Metadata')['href']
            metadata_link = f"https://www.pasda.psu.edu/uci/{metadata_href}"
            dl_tag = entry.parent.parent.find('a', string='Download')
            download_link = f"https://www.pasda.psu.edu/uci/{dl_tag['href']}" if dl_tag else ''
            landing_page = f"https://www.pasda.psu.edu/uci/{entry['href']}"
            identifier = 'pasda-' + landing_page.rsplit('=', 1)[-1]
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
        return pd.DataFrame(rows)

    def drop_federal(self, df):
        federal = [
            "United States Army Corps of Engineers USACE", "U S Geological Survey", "U S Fish and Wildlife Service",
            "U S Environmental Protection Agency", "U S Department of Justice", "U S Department of Commerce",
            "U S Department of Agriculture", "U S Census Bureau", "National Weather Service NOAA NWS",
            "National Renewable Energy Laboratory NREL", "National Park Service", "National Geodetic Survey",
            "National Aeronautics and Space Administration NASA", "Federal Emergency Management Agency"
        ]
        return df[~df['Creator'].isin(federal)].reset_index(drop=True)

    def format_date_ranges(self, df):
        def make_range(s):
            years = re.findall(r"(\d{4})", s)
            return f"{years[0]}-{years[-1]}" if years else s
        df['Date Range'] = df['Date Issued'].apply(make_range)
        return df

    def add_default_values(self, df):
        today = time.strftime('%Y-%m-%d')
        df['Identifier'] = df['information']
        defaults = {
            'Code': '08a-01',
            'Access Rights': 'Public',
            'Accrual Method': 'HTML',
            'Date Accessioned': today,
            'Language': 'eng',
            'Is Part Of': '08a-01',
            'Member Of': 'ba5cc745-21c5-4ae9-954b-72dd8db6815a',
            'Provider': 'Pennsylvania Spatial Data Access (PASDA)',
            'Format': 'File',
            'Resource Class': 'Datasets',
            'Publication State': 'published'
        }
        for col, val in defaults.items():
            df[col] = val
        return df

    def transform_titles(self, df):
        df[['Title', 'Spatial Coverage']] = df.apply(
            lambda row: pd.Series(self.transform_title(row)), axis=1
        )
        return df


    def transform_title(self, row):
        alt_title = row['Alternative Title']

        # Replace county names
        for county in self.counties_in_pennsylvania:
            if re.search(f"{county} County", alt_title, re.I):
                alt_title = re.sub(f"{county} County", f"[Pennsylvania--{county} County]", alt_title, flags=re.I, count=1)
                break
        else:
            # Replace city names
            for city in self.cities_in_pennsylvania:
                if re.search(rf"\b{city}\b", alt_title, re.I):
                    alt_title = re.sub(rf"\b{city}\b", f"[Pennsylvania--{city}]", alt_title, flags=re.I, count=1)
                    break
            else:
                # Replace generic PA mentions
                alt_title = re.sub(r"\b(PA|Pennsylvania)\b", "[Pennsylvania]", alt_title, flags=re.I, count=1)

        # Capture and move bracketed content
        bracket_content = re.findall(r'\[(.*?)\]', alt_title)
        if bracket_content:
            alt_title = re.sub(r'\[.*?\]', '', alt_title).strip()
            alt_title = f"{alt_title} [{bracket_content[0]}]"

        # Clean up phrases
        alt_title = re.sub(r"For\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The City Of\s+\[", "[", alt_title, flags=re.I)

        # Remove leading/trailing dashes
        alt_title = re.sub(r"^\s*-\s*|\s*-\s*(?=\[)", "", alt_title)

        # Capitalize first letter
        if alt_title:
            alt_title = alt_title[0].capitalize() + alt_title[1:]

        # Append Date Issued
        alt_title += f" {{{row['Date Issued']}}}"

        return alt_title, bracket_content[0] if bracket_content else ""


    def append_spatial_fields(self, df):
        """
        Merge on 'Spatial Coverage' to append Bounding Box, Geometry, and GeoNames.
        """
        merged_df = pd.merge(
            df, self.spatial_data, left_on='Spatial Coverage', right_on='County', how='left'
        )
        print("[PASDA] Merged harvested records with spatial data on 'Spatial Coverage'.")

        # Define Pennsylvania-wide defaults for unmatched records
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
                merged_df[column] = merged_df[column].fillna(default)
            else:
                merged_df[column] = default
            print(f"[PASDA] Set default for missing '{column}' values.")

        return merged_df



def main():
    """
    Run PASDA harvest standalone for local testing.
    """
    config_path = "config/pasda.yaml"
    schema_path = "schemas/geobtaa_schema.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    Harvester = PasdaHarvester(config, schema_path)
    Harvester.harvest()



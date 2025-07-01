import os
import time
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
from utils.constants import FIELD_ORDER

class PasdaExtractor:
    def __init__(self, config, schema):
        self.config = config
        self.schema = schema
        locations_path = self.config.get("locations_json", "data/locations.json")
        with open(locations_path, "r", encoding="utf-8") as f:
            locations = json.load(f)
        self.counties_in_pennsylvania = locations["counties_in_pennsylvania"]
        self.cities_in_pennsylvania = locations["cities_in_pennsylvania"]

    def fetch(self):
        html_path = self.config["input_html"]
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()

    def normalize(self, raw_html):
        df = self.parse_pasda_html(raw_html)
        df = (df.pipe(self.drop_federal)
                .pipe(self.format_date_ranges)
                .pipe(self.add_default_values)
                .pipe(self.transform_titles)
                .pipe(self.cleanup_and_reorder))
        return df.to_dict(orient='records')

    def fetch_links(self, raw_html):
        df = self.parse_pasda_html(raw_html)
        links = []
        for _, row in df.iterrows():
            for field in ['full_layer_description', 'download', 'metadata_html']:
                url = row.get(field)
                if url:
                    links.append({
                        'friendlier_id': row['ID'],
                        'reference_type': field,
                        'distribution_url': url,
                        'label': row.get('Format', '') if field == 'download' else ''
                    })
        return links

    def normalize_links(self, raw_links):
        return raw_links

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
            identifier = 'pasda-' + landing_page.rsplit('=',1)[-1]
            rows.append({
                'Creator': publisher,
                'Date Issued': date_issued,
                'Alternative Title': title,
                'Description': description,
                'metadata_html': metadata_link,
                'download': download_link,
                'full_layer_description': landing_page,
                'ID': identifier
            })
        return pd.DataFrame(rows)

    def drop_federal(self, df):
        federal = ["United States Army Corps of Engineers USACE", "U S Geological Survey", "U S Fish and Wildlife Service", "U S Environmental Protection Agency", "U S Department of Justice", "U S Department of Commerce", "U S Department of Agriculture", "U S Census Bureau", "National Weather Service NOAA NWS", "National Renewable Energy Laboratory NREL", "National Park Service", "National Geodetic Survey", "National Aeronautics and Space Administration NASA", "Federal Emergency Management Agency"]
        return df[~df['Creator'].isin(federal)].reset_index(drop=True)

    def format_date_ranges(self, df):
        def make_range(s):
            years = re.findall(r"(\\d{4})", s)
            return f"{years[0]}-{years[-1]}" if years else s
        df['Date Range'] = df['Date Issued'].apply(make_range)
        return df

    def add_default_values(self, df):
        today = time.strftime('%Y-%m-%d')
        defaults = {'Code': '08a-01', 'Access Rights': 'Public', 'Accrual Method': 'HTML', 'Date Accessioned': today, 'Language': 'eng', 'Is Part Of': '08a-01', 'Member Of': 'ba5cc745-21c5-4ae9-954b-72dd8db6815a', 'Provider': 'Pennsylvania Spatial Data Access (PASDA)', 'Identifier': df['full_layer_description'], 'Format': 'File', 'Resource Class': 'Datasets'}
        for col, val in defaults.items():
            df[col] = val
        return df

    def apply_title_transformation(self, df):
        df['Title'] = df.apply(self.generate_transformed_title, axis=1)
        return df

    def generate_transformed_title(self, row):
        alt_title = row['Alternative Title']
        # Replace county names
        for county in self.counties_in_pennsylvania:
            if re.search(f"{county} County", alt_title, re.I):
                alt_title = re.sub(f"{county} County", f"[Pennsylvania--{county} County]", alt_title, flags=re.I, count=1)
                break
        else:
            # Replace city names
            for city in self.cities_in_pennsylvania:
                if re.search(rf"\\b{city}\\b", alt_title, re.I):
                    alt_title = re.sub(rf"\\b{city}\\b", f"[Pennsylvania--{city}]", alt_title, flags=re.I, count=1)
                    break
            else:
                # Replace generic PA mentions
                alt_title = re.sub(r"\\b(PA|Pennsylvania)\\b", "[Pennsylvania]", alt_title, flags=re.I, count=1)

        # Capture and move bracketed content
        bracket_content = re.findall(r'\[(.*?)\]', alt_title)
        if bracket_content:
            alt_title = re.sub(r'\[.*?\]', '', alt_title).strip()
            alt_title = f"{alt_title} [{bracket_content[0]}]"

        # Clean up phrases with correctly escaped brackets
        alt_title = re.sub(r"For\\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The\\s+\[", "[", alt_title, flags=re.I)
        alt_title = re.sub(r"For The City Of\\s+\[", "[", alt_title, flags=re.I)

        # Remove leading/trailing dashes
        alt_title = re.sub(r"^\\s*-\\s*|\\s*-\\s*(?=\[)", "", alt_title)

        # Capitalize first letter
        if alt_title:
            alt_title = alt_title[0].capitalize() + alt_title[1:]

        # Append Date Issued
        alt_title += f" {{{row['Date Issued']}}}"

        return alt_title


    def cleanup_and_reorder(self, df):
        df = df.applymap(lambda x: x.strip('|- ') if isinstance(x, str) else x)
        cols = [c for c in FIELD_ORDER if c in df.columns]
        return df.reindex(columns=cols)

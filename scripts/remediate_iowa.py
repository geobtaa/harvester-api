import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse
import time
import csv
import random # <--- For randomized delays
import os     # <--- To check if the output file exists

# --- Configuration ---
INPUT_FILE = 'input_with_ids.csv' 
OUTPUT_FILE = 'output_with_ids.csv'
# --- End Configuration ---

def get_new_urls_stealth(old_url, driver):
    """
    Uses undetected-chromedriver to navigate, then explicitly waits for the URL to change.
    """
    old_url = old_url.strip()
    try:
        driver.get(old_url)
        WebDriverWait(driver, 20).until(EC.url_changes(old_url)) # Increased timeout to 20s
        new_landing_page_url = driver.current_url
        
        path_parts = urlparse(new_landing_page_url).path.split('/')
        if 'node' in path_parts and len(path_parts) > path_parts.index('node') + 1:
            node_id = path_parts[path_parts.index('node') + 1]
            new_iiif_manifest_url = f"https://digital.lib.uiowa.edu/node/{node_id}/iiif-p/manifest"
            return new_landing_page_url, new_iiif_manifest_url
        else:
            return 'Error: Could not parse node ID from final URL', new_landing_page_url

    except Exception as e:
        return f'Error: No redirect or process failed ({type(e).__name__})', ''

# --- Main Script Execution ---
if __name__ == "__main__":
    # --- Part 1: Setup for Resumability ---
    processed_ids = set()
    output_file_exists = os.path.exists(OUTPUT_FILE)
    
    if output_file_exists:
        try:
            with open(OUTPUT_FILE, 'r', newline='', encoding='utf-8') as f_out:
                reader = csv.DictReader(f_out)
                for row in reader:
                    processed_ids.add(row['ID'])
            print(f"Found {len(processed_ids)} items already processed in the output file. They will be skipped.")
        except (IOError, csv.Error) as e:
            print(f"Warning: Could not properly read existing output file. Starting fresh. Error: {e}")
            output_file_exists = False # Treat as if it doesn't exist if it's corrupt

    # --- Part 2: Initialize Browser ---
    print("Initializing Undetected ChromeDriver...")
    options = uc.ChromeOptions()
    # Adding a polite User-Agent to identify our script
    user_agent = "BTAA-Geoportal-Remediation/1.0 (+https://geo.btaa.org/; contact: geoportal@btaa.org"
    options.add_argument(f'--user-agent={user_agent}')
    
    driver = uc.Chrome(options=options)

    # --- Part 3: Main Processing Loop ---
    print(f"Reading data from: {INPUT_FILE}")
    
    try:
        with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            rows = list(reader)
            total_rows = len(rows)
            
            # Open the output file in append mode
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f_out:
                fieldnames = ['ID', 'old_url', 'new_landing_page_url', 'new_iiif_manifest_url']
                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                
                # Write the header only if the file is new
                if not output_file_exists or os.path.getsize(OUTPUT_FILE) == 0:
                    writer.writeheader()
                
                for i, row in enumerate(rows):
                    item_id = row['ID']
                    
                    # --- RESUME LOGIC ---
                    if item_id in processed_ids:
                        print(f"Skipping ({i+1}/{total_rows}) ID: {item_id} (already processed)")
                        continue
                        
                    old_url = row['old_url']
                    print(f"Processing ({i+1}/{total_rows}) ID: {item_id}")
                    
                    new_landing_page, new_iiif_manifest = get_new_urls_stealth(old_url, driver)
                    
                    # --- INCREMENTAL SAVE ---
                    writer.writerow({
                        'ID': item_id,
                        'old_url': old_url,
                        'new_landing_page_url': new_landing_page,
                        'new_iiif_manifest_url': new_iiif_manifest
                    })
                    
                    # --- RATE LIMITING ---
                    # Wait for a random time between 2 and 5 seconds
                    sleep_time = random.uniform(2, 5)
                    time.sleep(sleep_time)

    except FileNotFoundError:
        print(f"ERROR: Input file not found at '{INPUT_FILE}'")
    except KeyError as e:
        print(f"ERROR: A required column is missing from your CSV file: {e}")
    finally:
        # Ensure the browser closes even if there's an error
        driver.quit()
        print(f"\nProcess finished. Results are saved in: {OUTPUT_FILE}")
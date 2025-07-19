from base import BaseHarvester
import json

# Add project root to sys.path so "utils" can be imported correctly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestHarvester(BaseHarvester):
    def fetch(self):
        with open("test_data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    
    def parse(self, raw_data):
        # For testing: pass through raw_data unmodified
        return raw_data

    def build_dataframe(self, parsed_data):
        # Simple mapping for test
        import pandas as pd
        return pd.DataFrame(parsed_data)
    
if __name__ == "__main__":
    config = {
        "output_primary_csv": "primary_test.csv",
        "output_distributions_csv": None
    }
    harvester = TestHarvester(config)
    harvester.harvest_pipeline()
# 🧭 How to Create a New Harvester Module

Each harvester in this system inherits from `BaseHarvester` and implements methods tailored to the structure and quirks of a specific metadata source. Sources such as ArcGIS or PASDA are called "mysource" in this how to doc.

---

## 1. Create a New Module File

Create a new file in the `harvesters/` directory, named after the source:



---

## 2. Define the Harvester Class

Start your module with the following structure:

```
from harvesters.base import BaseHarvester

class MySourceHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)

```

Then override key lifecycle methods as needed:

1. load_schema()
2. fetch()
3. parse()
4. flatten()
5. build_dataframe()
6. derive_fields()
7. add_defaults()
8. add_provenance()
9. clean()
10. validate()
11. write_outputs()

Each one aligns with a step in the harvester pipeline defined in BaseHarvester.

## 3. Use Utility Functions

Call shared utilities to reduce duplication:

```

from utils.cleaner import spatial_cleaning
from utils.validation import validation_pipeline

def clean(self, df):
    df = spatial_cleaning(df)
    df = super().clean(df)
    return df

```

These apply generic formatting, validation, and field normalization.

## 4. Prefix Source-Specific Helpers

Add custom logic with a source-specific prefix and organize it in a clear section:

```
# --- MySource-Specific Field Derivation ---

def mysource_clean_description(self, df):
    ...

```

Then chain the methods in derive_fields():

```

df = (
    df.pipe(self.mysource_clean_description)
      .pipe(self.mysource_extract_dates)
)

```

This helps distinguish your source-specific logic from base functionality.

## 5. Add a Main Block (Optional)

For standalone testing, add a main() function at the bottom:

```

def main():
    import yaml
    config_path = "config/mysource.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    Harvester = MySourceHarvester(config)
    Harvester.harvest()

```

Run it directly with:

```
python harvesters/mysource.py
```

Naming Convention

Use these naming patterns to stay consistent:

Class: MySourceHarvester
File: mysource.py
Helper functions: mysource_ prefix

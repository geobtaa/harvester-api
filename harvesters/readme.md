## Understanding Our Metadata Harvesters

Our metadata harvesters are tools that gather records from different sources, like ArcGIS servers or other data websites, and prepare them for our collection. These harvesters are set up so they share a common process, but each source can have its own special steps when needed.

We have designed our harvesters like a recipe template:

- The common process (like the recipe steps) lives in one main file. It covers everything we almost always do: loading our schema, downloading data, turning it into a table, cleaning it up, checking for problems, and saving it to files.
- Each individual harvester (for example, for ArcGIS) builds on that common process. It can add or change steps in the recipe if needed, like adjusting the title of each record or dropping records missing important information.
- By setting them up this way, we only have to update the common parts of the process in one place, and every harvester will get those improvements automatically.
  
## Why we designed it like this

- It saves time: instead of copying and pasting the same process for every harvester, we have a single shared structure.
- It reduces mistakes: fixes or improvements to the main process instantly help every harvester.
- It’s easier to read: you only see what’s unique in each harvester, not pages of repeated code.

## What you’ll find in the harvesters

- A common process file, which contains the general steps that every harvester follows.
- Separate files for each harvester (like ArcGIS or PASDA), which focus only on what’s different or special about that source.

This design lets us handle both the shared and unique parts of harvesting metadata in a consistent way. Our goal is for future team members to understand what’s happening without needing to read every line of code.

## How to Create a New Harvester Module

Each harvester in this system inherits from `BaseHarvester` and implements methods tailored to the structure and quirks of a specific metadata source. Sources such as ArcGIS or PASDA are called "mysource" in this how to doc.

---

### 1. Create a New Module File

Create a new file in the `harvesters/` directory, named after the source:



---

### 2. Define the Harvester Class

Start your module with the following structure:

```
from harvesters.base import BaseHarvester

class MySourceHarvester(BaseHarvester):
    def __init__(self, config):
        super().__init__(config)

```

Then override key lifecycle methods as needed:

1. load_reference_data()
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

### 3. Use Utility Functions

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

### 4. Prefix Source-Specific Helpers

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

### 5. Add a Main Block (Optional)

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


More on each class:

#### fetch()

Purpose:
Responsible for retrieving raw metadata. This might be:

- A local directory of JSON files
- An API response (JSON, XML, HTML, etc.)
- A remote file (like CSV or zipped archive)

When to use:
Always—every harvester needs some way to get raw input.

What it returns:
Usually a list of raw items (e.g., raw JSON strings, HTML blocks, or binary blobs).

#### parse()

Purpose:
Convert raw input into structured Python objects (dicts/lists). For example:

- Convert JSON strings into dict
- Use BeautifulSoup to turn HTML into structured data
- Normalize inconsistent formats

When to use:
When the result of fetch() is not already a structured dict or list.

- If fetch() loads JSON files with json.load(), parse() is not necessary and may just return the input.
- If fetch() retrieves raw HTML or bytes, parse() is essential.

What it returns:
List of structured Python dictionaries (or a single dict/list).
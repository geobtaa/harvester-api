# Metadata Harvester API Toolkit

This repository contains the **early framework** for a new, API-driven metadata harvesting toolkit designed to support our evolving metadata infrastructure.

- Built on **FastAPI** for orchestration  
- Implements a modular **extractor architecture**, starting with ArcGIS Hub harvesting  
- Centralizes the metadata schema in a single YAML file, ensuring consistency across harvesters  
- Provides a lightweight **admin interface** for running jobs via a web browser  

---

## Current Features

- `/schema` endpoint: serves our canonical metadata schema  
- `/jobs` endpoint: lists available harvesting jobs  
- `/jobs/{id}/run` endpoint: executes harvesters on demand  
- Basic admin web UI for non-developer users to trigger harvests  

---

## Status

This is an **early proof of concept** to demonstrate the integration of our metadata harvesting workflows with a modern API stack. Functionality is limited but lays the foundation for future enhancements, including:

- Adding more extractor types (e.g., CKAN, Socrata)  
- Incorporating schema validation and enrichment  
- Integrating with a metadata editor  

---

## Directory Overview

```
- data/          # Input files (e.g., ArcGIS Hub lists)
- extractors/    # Extractor modules (ArcGIS implemented)
- jobs/          # YAML configs for each harvesting job
- outputs/       # Harvested metadata CSV outputs
- routers/       # FastAPI endpoints
- schemas/       # Canonical metadata schema
- static/        # Admin web UI
- utils/         # Shared utilities
- main.py        # FastAPI app entry point
```

## Setup instructions

1. Clone the repository and change into this directory
2. Install dependencies with: `pip install -r requirements.txt`
3. Start the FastAPI Server: `uvicorn main:app --reload`
4. Review the API documentation (Swagger UI) at http://localhost:8000/docs
5. For a list of runnable jobs, go to http://localhost:8000/


**Notes:**

* The --reload flag automatically restarts the server when you edit code.
* Jobs are configured in YAML files inside the jobs/ directory.
* Outputs from harvests will be saved in the outputs/ folder.


## Adding jobs

To create new harvesters, here are the basic steps:

1. Add a new Python file in the `extractors/` directory
2. Create a job config YAML in `jobs/`
3. In `routers/jobs.py`, update the run endpoint for the new extractor type
4. Test the new harvester

*More details tbd*
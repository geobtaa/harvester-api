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

## ⚙Status

This is an **early proof of concept** to demonstrate the integration of our metadata harvesting workflows with a modern API stack. Functionality is limited but lays the foundation for future enhancements, including:

- Adding more extractor types (e.g., CKAN, Socrata)  
- Incorporating schema validation and enrichment  
- Integrating with our upcoming metadata editor  

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

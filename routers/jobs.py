from fastapi import APIRouter, HTTPException
import os
import yaml
import time

from utils.file_io import load_local_schema, write_csv
from extractors.arcgis import ArcGISExtractor
from extractors.pasda import PasdaExtractor

router = APIRouter()

@router.get("/jobs")
async def list_jobs():
    """
    List available jobs by scanning the jobs/ folder.
    """
    job_files = sorted(f for f in os.listdir("jobs") if f.endswith(".yaml"))
    jobs = []
    for filename in job_files:
        job_id = os.path.splitext(filename)[0]
        config = yaml.safe_load(open(os.path.join("jobs", filename), encoding="utf-8"))
        jobs.append({
            "id": job_id,
            "name": config.get("name", job_id)
        })
    return jobs

@router.post("/jobs/{job_id}/run")
async def run_job(job_id: str):
    """
    Run a harvesting job by ID.
    """
    config_path = os.path.join("jobs", f"{job_id}.yaml")
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    # Load job config & schema
    job_cfg = yaml.safe_load(open(config_path, encoding="utf-8"))
    schema = load_local_schema()

    # Instantiate the right extractor
    extractor_type = job_cfg.get("type")
    if extractor_type == "arcgis":
        extractor = ArcGISExtractor(job_cfg, schema)
    elif extractor_type == "pasda":
        extractor = PasdaExtractor(job_cfg, schema)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported extractor type '{extractor_type}'")

    results = {}
    today = time.strftime("%Y-%m-%d")

    # 1) Metadata CSV
    records = extractor.fetch()
    normalized = extractor.normalize(records)
    meta_out = job_cfg["output_metadata_csv"]
    dated_meta = os.path.join("outputs", f"{today}_{os.path.basename(meta_out)}")
    write_csv(normalized, dated_meta)
    results["metadata_csv"] = dated_meta

    # 2) Links CSV (if supported)
    links_cfg = job_cfg.get("output_links_csv")
    if links_cfg and hasattr(extractor, "fetch_links") and hasattr(extractor, "normalize_links"):
        raw_links = extractor.fetch_links(records)
        norm_links = extractor.normalize_links(raw_links)
        dated_links = os.path.join("outputs", f"{today}_{os.path.basename(links_cfg)}")
        write_csv(norm_links, dated_links)
        results["links_csv"] = dated_links

    return {"status": "completed", **results}

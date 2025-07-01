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

    # 1) Primary metadata CSV
    records = extractor.fetch()
    normalized_records = extractor.normalize(records)
    primary_out = job_cfg["output_primary_csv"]
    dated_primary = os.path.join("outputs", f"{today}_{os.path.basename(primary_out)}")
    write_csv(normalized_records, dated_primary)
    results["primary_csv"] = dated_primary

# 2) Distributions CSV (if configured)
    distributions_cfg = job_cfg.get("output_distributions_csv")
    if distributions_cfg:
        import pandas as pd
        normalized_df = pd.DataFrame(normalized_records)
        distributions_df = extractor.generate_secondary_table(normalized_df)
        dated_distributions = os.path.join("outputs", f"{today}_{os.path.basename(distributions_cfg)}")
        distributions_df.to_csv(dated_distributions, index=False)
        results["distributions_csv"] = dated_distributions

    return {"status": "completed", **results}


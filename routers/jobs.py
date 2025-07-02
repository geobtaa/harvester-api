from fastapi import APIRouter, HTTPException
import os
import yaml
import time
import pandas as pd

from utils.file_io import load_local_schema, write_csv
from utils.distribution_writer import generate_secondary_table
from utils.constants import PRIMARY_FIELD_ORDER
# from extractors.base import load_local_schema
from extractors.pasda import PasdaExtractor
from extractors.arcgis import ArcGISExtractor

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
    # Load job configuration
    config_path = os.path.join("jobs", f"{job_id}.yaml")
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    with open(config_path, encoding="utf-8") as cfg:
        job_cfg = yaml.safe_load(cfg)

    # Load schema and instantiate extractor
    schema = load_local_schema()
    extractor_type = job_cfg.get("type")

    if extractor_type == "arcgis":
        extractor = ArcGISExtractor(job_cfg, schema)
    elif extractor_type == "pasda":
        extractor = PasdaExtractor(job_cfg, schema)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported extractor type '{extractor_type}'"
        )

    # Fetch and normalize
    fetched = extractor.fetch()
    primary_records, secondary_records = extractor.normalize(fetched)

    # Prepare outputs
    today = time.strftime("%Y-%m-%d")
    results = {}

    # Primary metadata CSV
    primary_df = pd.DataFrame(primary_records)
    # Reorder to primary schema fields only
    primary_df = primary_df.reindex(
        columns=[c for c in PRIMARY_FIELD_ORDER if c in primary_df.columns]
    )
    primary_out = job_cfg["output_primary_csv"]
    dated_primary = os.path.join(
        "outputs", f"{today}_{os.path.basename(primary_out)}"
    )
    primary_df.to_csv(dated_primary, index=False)
    results["primary_csv"] = dated_primary

    # Distributions CSV (if configured)
    dist_cfg = job_cfg.get("output_distributions_csv")
    if dist_cfg:
        secondary_df = pd.DataFrame(secondary_records)
        dated_dist = os.path.join(
            "outputs", f"{today}_{os.path.basename(dist_cfg)}"
        )
        secondary_df.to_csv(dated_dist, index=False)
        results["distributions_csv"] = dated_dist

    return {"status": "completed", **results}




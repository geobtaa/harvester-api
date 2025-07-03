from fastapi import APIRouter, HTTPException
import os
import yaml

from utils.file_io import load_local_schema
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
        config_path = os.path.join("jobs", filename)
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
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

    with open(config_path, encoding="utf-8") as f:
        job_cfg = yaml.safe_load(f)

    # Load schema and instantiate the correct extractor
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

    # Run the full extractor workflow (fetch, normalize, write outputs)
    results = extractor.extract()

    return {"status": "completed", **results}




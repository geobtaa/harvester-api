from fastapi import APIRouter, HTTPException
import os
import yaml

from utils.file_io import load_local_schema
from harvesters.pasda import PasdaHarvester
from harvesters.arcgis import ArcGISHarvester

router = APIRouter()

@router.get("/jobs")
async def list_jobs():
    """
    List available harvesting jobs by scanning the config/ folder.
    """
    config_files = sorted(f for f in os.listdir("config") if f.endswith(".yaml"))
    jobs = []
    for filename in config_files:
        job_id = os.path.splitext(filename)[0]
        config_path = os.path.join("config", filename)
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
    Run a harvesting job by ID, loading its configuration from the config/ folder.
    """
    # Load job configuration
    config_path = os.path.join("config", f"{job_id}.yaml")
    if not os.path.exists(config_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    with open(config_path, encoding="utf-8") as f:
        job_cfg = yaml.safe_load(f)

    # Load schema and instantiate the correct harvester
    # schema = load_local_schema()
    harvester_type = job_cfg.get("type")

    if harvester_type == "arcgis":
        harvester = ArcGISHarvester(job_cfg)
    elif harvester_type == "pasda":
        harvester = PasdaHarvester(job_cfg)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported harvester type '{harvester_type}'"
        )

    # Run the full harvester workflow (fetch, normalize, write outputs)
    results = harvester.harvest_pipeline()

    return {"status": "completed", **results}

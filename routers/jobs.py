from fastapi import APIRouter, HTTPException
import os
import yaml
from utils.file_io import load_local_schema, write_csv
from extractors.arcgis import ArcGISExtractor  # import your extractor
import time

router = APIRouter()

@router.get("/jobs")
async def list_jobs():
    """
    List available jobs by scanning the jobs/ folder.
    """
    job_files = [f for f in os.listdir("jobs") if f.endswith(".yaml")]
    jobs = []

    for i, filename in enumerate(sorted(job_files), start=1):
        job_id = os.path.splitext(filename)[0]
        # Optional: load the job name from the YAML if you want more details
        with open(os.path.join("jobs", filename), "r", encoding="utf-8") as f:
            job_config = yaml.safe_load(f)
        jobs.append({
            "id": job_id,
            "name": job_config.get("name", "Unnamed job")
        })

    return jobs

@router.post("/jobs/{job_id}/run")
async def run_job(job_id: str):
    """
    Run a harvesting job by ID.
    """
    job_config_path = f"jobs/{job_id}.yaml"

    if not os.path.exists(job_config_path):
        raise HTTPException(status_code=404, detail=f"Job config {job_id} not found")

    # Load the job config YAML
    with open(job_config_path, "r", encoding="utf-8") as f:
        job_config = yaml.safe_load(f)

    # Load your canonical schema
    schema = load_local_schema()

    # Choose the extractor based on job_config["type"]
    extractor_type = job_config.get("type")
    if extractor_type == "arcgis":
        extractor = ArcGISExtractor(job_config, schema)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported extractor type: {extractor_type}")

    # Fetch raw data from source
    fetched_records = extractor.fetch()

    # Normalize records to your local schema
    normalized_records = extractor.normalize(fetched_records)

    # Write the normalized metadata CSV
    
    # Prepend date to output path
    today = time.strftime("%Y-%m-%d")
    orig_output_path = job_config["output_metadata_csv"]
    dated_output_path = f"outputs/{today}_{os.path.basename(orig_output_path)}"

    write_csv(normalized_records, dated_output_path)

    # TODO: link fields that need a second CSV

    return {"status": "completed", "metadata_csv": dated_output_path}

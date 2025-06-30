from fastapi import APIRouter, HTTPException
from utils.file_io import load_local_schema

router = APIRouter()

@router.get("/schema")
async def get_schema():
    """Return the current local metadata schema."""
    schema = load_local_schema()
    return schema
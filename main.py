from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Include routers
from routers import schema as schema_router
from routers import jobs as jobs_router

app.include_router(schema_router.router)
app.include_router(jobs_router.router)

# Serve static files at root path
app.mount("/", StaticFiles(directory="static", html=True), name="static")



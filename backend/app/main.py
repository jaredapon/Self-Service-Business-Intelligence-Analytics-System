from fastapi import FastAPI
from app.api import upload  # Import the router from upload.py

# Create the main FastAPI application instance
app = FastAPI(
    title="File Upload & Pipeline Service",
    description="Handles file uploads and triggers a processing pipeline."
)

# Include the /upload router
# All routes defined in app.api.upload will now be prefixed with /upload
app.include_router(upload.router, prefix="/upload", tags=["Upload"])

# A simple root endpoint to check if the API is running
@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "API is running."}
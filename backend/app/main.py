"""
This module creates and configures the main FastAPI application instance.
It acts as the central point for assembling the different parts of the API,
such as routers and middleware.
"""
from fastapi import FastAPI
from app.api import upload  # Import the router from the 'upload.py' API module.

# Create the main FastAPI application instance.
# The 'title' and 'description' parameters are used to generate the API documentation
# (which is available at /docs and /redoc by default).
app = FastAPI(
    title="File Upload & Pipeline Service",
    description="Handles file uploads and triggers a data processing pipeline."
)

# Include the router from the 'upload' module.
# This makes all the API routes defined in 'upload.router' available to the main app.
# - prefix="/upload": All routes from the 'upload' router will be prefixed with '/upload'.
#   For example, a route defined as @router.post("/upload") in upload.py will become
#   available at POST /upload/upload.
# - tags=["Upload"]: This groups the routes under the "Upload" tag in the API docs.
app.include_router(upload.router, prefix="/upload", tags=["Upload"])

# A simple root endpoint (GET /) to serve as a health check.
# It can be used to quickly verify if the API server is running and responsive.
@app.get("/", tags=["Root"])
async def read_root():
    """A simple root endpoint to check if the API is running."""
    return {"message": "API is running."}
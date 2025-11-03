"""
This module creates and configures the main FastAPI application instance.
It acts as the central point for assembling the different parts of the API,
such as routers and middleware.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import upload  # Import the router from the 'upload.py' API module.
from app.core.config import settings

# Create the main FastAPI application instance.
# ...
app = FastAPI(
    title="File Upload & Pipeline Service",
    description="Handles file uploads and triggers a data processing pipeline."
)

# Add CORS middleware to allow the frontend to communicate with the API.
# This is crucial for a browser-based frontend.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.cors_origin],  # Allows your frontend's origin
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


# Include the router from the 'upload' module.
# ...
app.include_router(upload.router, prefix="/upload", tags=["Upload"])

# ...
@app.get("/", tags=["Root"])
async def read_root():
    """A simple root endpoint to check if the API is running."""
    return {"message": "API is running."}
"""
This module defines the API endpoint for handling file uploads.
It follows the flow:
1. Receives files from a user via a POST request.
2. Verifies the user's JWT for authentication.
3. Validates that the files are of an allowed type (.csv, .xlsx).
4. Uploads each valid file to a MinIO object storage bucket.
5. Creates a '_complete' marker file in MinIO to signal the batch is finished.
6. Creates a local trigger file that a watchdog service monitors.
7. The watchdog service then initiates the data processing pipeline.
"""
import os
import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from typing import List

from app.services.auth import get_current_user
from app.services.storage import upload_file_to_minio, create_complete_marker
from app.core.config import settings

# Create an APIRouter
router = APIRouter()

# --- Setup logging ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# --- Helper function for file validation ---
def validate_file_type(filename: str) -> bool:
    """
    Validates the file type based on its extension.
    Allows only .csv and .xlsx files.
    """
    ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
    _, ext = os.path.splitext(filename)
    return ext.lower() in ALLOWED_EXTENSIONS


@router.post("/upload")
async def handle_file_upload(
    files: List[UploadFile] = File(...),
    # The 'Depends' on get_current_user handles JWT validation.
    # If the token is invalid, it will raise a 401 Unauthorized error automatically.
    current_user: dict = Depends(get_current_user)
):
    """
    Handles the main file upload flow. It authenticates the user, validates
    files, uploads them to MinIO, and triggers the processing pipeline.
    """
    log.info(f"Upload request received from user: {current_user.get('email')}")

    uploaded_files = []

    for file in files:
        # --- 1. Validates file types ---
        if not validate_file_type(file.filename):
            log.warning(f"Invalid file type skipped: {file.filename}")
            continue  # Skip this file and move to the next

        try:
            # --- 2. Uploads files to MinIO ---
            # The storage service handles creating a unique name
            file_size = await upload_file_to_minio(file, file.filename)
            uploaded_files.append(file.filename)
            log.info(f"Successfully uploaded {file.filename} ({file_size} bytes) to MinIO.")

        except Exception as e:
            log.error(f"Failed to upload {file.filename}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to upload file: {file.filename}"
            )

    if not uploaded_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid files were uploaded. Please upload .csv or .xlsx files."
        )

    try:
        # --- 3. Creates "complete" marker in MinIO ---
        await create_complete_marker()
        log.info("Created '_complete' marker in MinIO.")

        # --- 4. Triggers pipeline by creating the local trigger file ---
        # Ensure the trigger directory exists before writing to it
        os.makedirs(settings.trigger_dir, exist_ok=True)
        trigger_file_path = os.path.join(settings.trigger_dir, "complete")
        with open(trigger_file_path, 'w') as f:
            f.write('trigger')
        log.info(f"Created local trigger file at: {trigger_file_path}")

    except Exception as e:
        log.error(f"Failed during trigger or marker creation: {e}")
        # Note: You might want to add logic here to clean up the files if this part fails
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Files uploaded but failed to trigger pipeline."
        )

    # --- 5. Returns success response ---
    return {
        "message": "Files uploaded successfully and pipeline triggered.",
        "uploaded_files": uploaded_files
    }
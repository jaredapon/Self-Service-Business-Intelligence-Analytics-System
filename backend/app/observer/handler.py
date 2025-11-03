"""
This module defines the event handler for the watchdog observer.
The `PipelineEventHandler` class contains the logic that executes when
a file system event (specifically, the creation of a 'complete' file)
is detected in the monitored directory.
"""

import logging
import os
import time
from watchdog.events import FileSystemEventHandler
from app.pipeline import run_all  # Import the main pipeline runner function.
from app.core.config import settings

class PipelineEventHandler(FileSystemEventHandler):
    """
    Handles file system events in the trigger directory. When the 'complete'
    file is created, it triggers the data processing pipeline.
    """
    def __init__(self):
        """Initializes the handler and its logger."""
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        """
        This method is called by the watchdog observer when a new file or
        directory is created in the monitored path.
        """
        # First, ignore events that are for directory creation. We only care about files.
        if event.is_directory:
            return

        # Check if the name of the created file is exactly "complete".
        # This is our specific trigger file.
        if os.path.basename(event.src_path) == "complete":
            self.logger.info(f"--- Trigger file detected: {event.src_path} ---")
            
            # It's good practice to wait a very short moment. This can help ensure
            # that the file write operation is fully finished before we try to act on it.
            time.sleep(0.5) 
            
            try:
                # --- Runs ETL -> MBA -> PED -> Holt-Winters ---
                self.logger.info("Starting pipeline execution...")
                run_all.execute_pipeline()
                self.logger.info("--- Pipeline execution finished. ---")
                
            except Exception as e:
                # If the pipeline fails for any reason, log the error.
                self.logger.error(f"Pipeline execution failed: {e}")
                
            finally:
                # CRITICAL STEP: Always attempt to remove the trigger file,
                # whether the pipeline succeeded or failed. This "resets" the
                # system, allowing it to be triggered again by a future upload.
                try:
                    os.remove(event.src_path)
                    self.logger.info(f"Cleaned up trigger file: {event.src_path}")
                except Exception as e:
                    self.logger.error(f"Failed to remove trigger file: {e}")

    def on_modified(self, event):
        """
        This method is called when a file is modified. We don't need to act on
        modifications for this workflow, so it's left empty.
        """
        pass
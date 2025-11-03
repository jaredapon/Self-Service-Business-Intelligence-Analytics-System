# /backend/app/observer/handler.py

import logging
import os
import time
from watchdog.events import FileSystemEventHandler
from app.pipeline import run_all # Import the main pipeline runner
from app.core.config import settings

class PipelineEventHandler(FileSystemEventHandler):
    """
    Handles file system events in the trigger directory.
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        """
        Called when a file is created.
        """
        # Ignore directories
        if event.is_directory:
            return

        # Check if the created file is our trigger file
        if os.path.basename(event.src_path) == "complete":
            self.logger.info(f"--- Trigger file detected: {event.src_path} ---")
            
            # Wait a moment for the file write to be fully complete
            time.sleep(0.5) 
            
            try:
                # --- Runs ETL -> MBA -> PED -> Holt-Winters ---
                self.logger.info("Starting pipeline execution...")
                run_all.execute_pipeline()
                self.logger.info("--- Pipeline execution finished. ---")
                
            except Exception as e:
                self.logger.error(f"Pipeline execution failed: {e}")
                
            finally:
                # Clean up the trigger file so we can be triggered again
                try:
                    os.remove(event.src_path)
                    self.logger.info(f"Cleaned up trigger file: {event.src_path}")
                except Exception as e:
                    self.logger.error(f"Failed to remove trigger file: {e}")

    def on_modified(self, event):
        # You can also watch for modifications if needed
        pass
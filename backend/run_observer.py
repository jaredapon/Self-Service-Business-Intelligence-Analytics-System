"""
This script is the entry point for the watchdog observer service.
It continuously monitors a specified directory for a trigger file ('complete').
When the trigger file is detected, it initiates the data processing pipeline.
This service is intended to run as a separate, long-running process,
typically in its own Docker container.
"""
import time
import logging
import os
from watchdog.observers import Observer
from app.observer.handler import PipelineEventHandler
from app.core.config import settings

# Set up basic logging to provide visibility into the observer's status and actions.
# The format includes a timestamp, making it easy to track when events occur.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
    # Retrieve the directory to watch from the application's central settings.
    path = settings.trigger_dir
    
    # Defensive check: Ensure the trigger directory exists before starting.
    # This prevents the observer from crashing if the directory is missing on startup.
    if not os.path.exists(path):
        logging.warning(f"Trigger directory {path} not found! Creating it.")
        os.makedirs(path)
        
    # Instantiate the event handler, which contains the logic for what to do
    # when a file event (like creation) occurs.
    event_handler = PipelineEventHandler()
    
    # Create an observer instance.
    observer = Observer()
    
    # Schedule the observer to watch the specified path.
    # - event_handler: The object that will receive event notifications.
    # - path: The directory to monitor.
    # - recursive=False: We only care about the top-level directory, not subdirectories.
    observer.schedule(event_handler, path, recursive=False)
    
    # Start the observer thread. It will now run in the background.
    observer.start()
    logging.info(f"Starting observer, watching directory: {path}")
    
    try:
        # Keep the main thread alive. The observer is running in a background thread.
        # This loop prevents the script from exiting immediately.
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C.
        logging.info("Observer stopping...")
        observer.stop()
    except Exception as e:
        # Catch any other unexpected errors to ensure the observer is stopped.
        logging.error(f"Observer encountered an error: {e}")
        observer.stop()
    
    # Wait for the observer thread to finish its work before exiting the script.
    observer.join()
    logging.info("Observer shut down.")
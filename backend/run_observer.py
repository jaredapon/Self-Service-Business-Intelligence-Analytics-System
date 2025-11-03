import time
import logging
from watchdog.observers import Observer
from app.observer.handler import PipelineEventHandler  # Assuming your handler class is named this
from app.core.config import settings

# Set up basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
    path = settings.TRIGGER_DIR
    
    # Ensure the trigger directory exists
    import os
    if not os.path.exists(path):
        logging.warning(f"Trigger directory {path} not found! Creating it.")
        os.makedirs(path)
        
    event_handler = PipelineEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    
    observer.start()
    logging.info(f"Starting observer, watching directory: {path}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Observer stopping...")
        observer.stop()
    except Exception as e:
        logging.error(f"Observer encountered an error: {e}")
        observer.stop()
    
    observer.join()
    logging.info("Observer shut down.")
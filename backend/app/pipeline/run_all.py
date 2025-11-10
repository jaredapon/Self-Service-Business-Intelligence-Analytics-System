"""
This module acts as the main orchestrator for the data processing pipeline.
It defines a single function, `execute_pipeline`, which calls each step
of the pipeline in the correct sequence.
"""

import logging
import time

# --- Import your actual pipeline scripts ---
from . import etl
from . import mba
from . import ped
from . import nlp
from . import holtwinters

def execute_pipeline():
    """
    Runs the full data processing pipeline in sequence:
    1. ETL (Extract, Transform, Load)
    2. MBA (Market Basket Analysis)
    3. PED (Price Elasticity of Demand)
    4. NLP (Non-Linear Programming for Price Optimization)
    5. Holt-Winters (Forecasting)
    """
    logger = logging.getLogger(__name__)
    
    pipeline_steps = [
        ("ETL", etl.main),
        ("MBA", mba.main),
        ("PED", ped.main),
        ("NLP", nlp.main),
        ("Holt-Winters", holtwinters.main),
    ]

    total_start_time = time.time()
    logger.info("--- Starting full data pipeline execution ---")

    try:
        for i, (name, func) in enumerate(pipeline_steps):
            step_start_time = time.time()
            logger.info(f"--- Step {i+1}/{len(pipeline_steps)}: Running {name}... ---")
            
            func() # Execute the main function of the script
            
            step_duration = time.time() - step_start_time
            logger.info(f"--- Step {i+1}/{len(pipeline_steps)}: {name} completed in {step_duration:.2f} seconds. ---")

        total_duration = time.time() - total_start_time
        logger.info(f"--- Full data pipeline completed successfully in {total_duration:.2f} seconds. ---")
        
    except Exception as e:
        # If any step in the pipeline fails, log the error and re-raise the exception.
        # This ensures the failure is recorded and can be handled by the calling service (the observer).
        logger.error(f"--- Error during pipeline execution at step '{name}': {e} ---")
        import traceback
        traceback.print_exc()
        raise e
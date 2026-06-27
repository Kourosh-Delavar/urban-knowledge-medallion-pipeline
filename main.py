import sys
import time
from src.utils.logger import get_logger

# import silver and gold layer builders
from src.silver.h3_spatial_builder import build_silver_layer
from src.gold.llm_context_graph import build_gold_layer

logger = get_logger(__name__)

def run_pipeline():

    start_time = time.time()

    # silver stage
    logger.info("[Silver Stage] running silver layer process...")
    silver_start = time.time()

    try:
        build_silver_layer()
        silver_duration = time.time() - silver_start
        logger.info(f"[SUCCESS] Silver stage successfully processed and saved to Iceberg in duration {silver_duration:.2f} seconds.")
    except Exception as e:
        logger.fatal(f"[FATAL ERROR] Silver builder failed: {str(e)}")
        sys.exit(1)

    # gold stage
    logger.info("[Gold Stage] running gold layer process...")
    gold_start = time.time()

    try:
        build_gold_layer()
        gold_duration = time.time() - gold_start
        logger.info(f"[SUCCESS] Gold stage local vector embeddings successfully synthesized in duration {gold_duration:.2f} seconds.")
    except Exception as e:
        logger.fatal(f"[FATAL ERROR] Gold builder failed: {str(e)}")
        sys.exit(1)

    total_duration = time.time() - start_time
    logger.info("fTotal execution time: {total_duration:.2f} seconds.")

if __name__ == "__main__":
    run_pipeline()
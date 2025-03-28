import os
import time
import asyncio
from dotenv import load_dotenv
from loguru import logger

from src.config import Config
from src.aggregator import Aggregator

async def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize configuration
    config = Config()
    logger.info(f"Starting worker {config.worker_id}")
    
    # Initialize aggregator
    aggregator = Aggregator(config)
    
    # Main loop
    try:
        while True:
            logger.info("Starting aggregation cycle")
            await aggregator.run()
            logger.info(f"Sleeping for {config.polling_interval} seconds")
            await asyncio.sleep(config.polling_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down worker")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())

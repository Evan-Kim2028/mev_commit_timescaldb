import asyncio
from contextlib import asynccontextmanager
import logging
import os
import polars as pl
from hypermanager.manager import HyperManager
from hypermanager.protocols.mev_commit import mev_commit_config
from dotenv import load_dotenv

from pipeline.db import create_connection, write_events_to_timescale, get_max_block_number
from pipeline.queries import fetch_event_for_config

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}


@asynccontextmanager
async def get_manager():
    """Context manager for HyperManager"""
    manager = HyperManager("https://mev-commit.hypersync.xyz")
    try:
        yield manager
    finally:
        await manager.close()  # if there's a close method


async def process_event_config(conn, manager, config, start_block: int = 0):
    """
    Process a single event configuration.

    Args:
        conn: Database connection
        manager: HyperManager instance
        config: Event configuration
        start_block: Starting block number
    """
    try:
        # Get the latest block number from the database
        max_block = get_max_block_number(conn, config.name)
        current_block = max(max_block, start_block)
        logger.info(f"Processing {config.name} starting from block {
                    current_block}")

        # Fetch events
        df: pl.DataFrame = await fetch_event_for_config(
            manager=manager,
            base_event_config=config,
            block_number=current_block+1
        )

        if df is not None and not df.is_empty():
            logger.info(f"Fetched {len(df)} rows for {config.name}")

            # Write to TimescaleDB
            write_events_to_timescale(conn, df, config.name)
            logger.info(f"Successfully wrote data to table {config.name}")
        else:
            logger.info(f"No new data to write for {config.name}")

    except Exception as e:
        logger.error(f"Error processing {config.name}: {e}", exc_info=True)
        # Continue with other configs even if one fails


async def main():
    """
    Main function to continuously fetch and store event data
    """
    logger.info("Starting TimescaleDB pipeline")

    conn = None
    try:
        # Create database connection
        conn = create_connection(DB_PARAMS)

        async with get_manager() as manager:
            while True:  # Continuous loop
                logger.info("Starting new fetch cycle")

                # Process all configurations in parallel
                tasks = []
                for config in mev_commit_config.values():
                    task = process_event_config(conn, manager, config)
                    tasks.append(task)

                # Wait for all tasks to complete
                await asyncio.gather(*tasks)

                logger.info(
                    "Completed fetch cycle, waiting for next iteration")
                # Wait for 30 seconds before next cycle
                await asyncio.sleep(30)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal, closing connections...")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())

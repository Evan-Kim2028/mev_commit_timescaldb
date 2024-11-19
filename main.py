import asyncio
from contextlib import asynccontextmanager
import logging
import os
import polars as pl
from hypermanager.manager import HyperManager
from hypermanager.protocols.mev_commit import mev_commit_config
from dotenv import load_dotenv

from pipeline.db import write_events_to_timescale, get_max_block_number, DatabaseConnection
from pipeline.queries import fetch_event_for_config
from pipeline.materialized_views import MaterializedViewManager

# Configure logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
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
        await manager.close()  # Add close method if available


async def process_event_config(conn, manager, config, start_block: int = 0):
    """Process a single event configuration."""
    try:
        max_block = get_max_block_number(conn, config.name)
        current_block = max(max_block, start_block)
        logger.info(f"Processing {config.name} starting from block {
                    current_block}")

        df: pl.DataFrame = await fetch_event_for_config(
            manager=manager,
            base_event_config=config,
            block_number=current_block+1
        )

        if df is not None and not df.is_empty():
            logger.info(f"Fetched {len(df)} rows for {config.name}")
            write_events_to_timescale(conn, df, config.name)
            logger.info(f"Successfully wrote data to table {config.name}")
        else:
            logger.info(f"No new data to write for {config.name}")

    except Exception as e:
        logger.error(f"Error processing {config.name}: {
                     str(e)}", exc_info=True)
        raise  # Re-raise to handle in main loop


async def process_batch(conn, manager, configs):
    """Process a batch of configurations with proper error handling."""
    try:
        tasks = [
            process_event_config(conn, manager, config)
            for config in configs
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}", exc_info=True)


async def main():
    """Main function to continuously fetch and store event data"""
    logger.info("Starting TimescaleDB pipeline")

    retry_count = 0
    max_retries = 3
    retry_delay = 5  # seconds

    while retry_count < max_retries:
        db = None
        try:
            db = DatabaseConnection(DB_PARAMS)
            conn = db.get_connection()
            view_manager = MaterializedViewManager(conn)

            # Initialize materialized views
            with db.autocommit():
                if not view_manager.create_preconf_txs_view():
                    logger.error(
                        "Failed to create preconf_txs materialized view")
                    return

            async with get_manager() as manager:
                while True:
                    logger.info("Starting new fetch cycle")
                    try:
                        # Process events in transaction mode
                        with db.transaction():
                            await process_batch(conn, manager, mev_commit_config.values())

                        # Refresh views in autocommit mode
                        with db.autocommit():
                            view_manager.refresh_materialized_views()

                        # Reset retry count on successful iteration
                        retry_count = 0

                    except Exception as e:
                        logger.error(f"Cycle error: {str(e)}", exc_info=True)
                        # Don't exit the main loop for individual cycle errors

                    logger.info(
                        "Completed fetch cycle, waiting for next iteration")
                    await asyncio.sleep(30)

        except Exception as e:
            retry_count += 1
            logger.error(f"Main loop error (attempt {retry_count}/{max_retries}): {str(e)}",
                         exc_info=True)

            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Max retries reached, exiting...")
                break

        finally:
            if db:
                try:
                    db.close()
                    logger.info("Database connection closed")
                except Exception as e:
                    logger.error(
                        f"Error closing database connection: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, closing gracefully...")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        exit(1)

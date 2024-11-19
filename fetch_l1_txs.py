import asyncio
from contextlib import asynccontextmanager
import logging
import os
from datetime import datetime
import polars as pl
from hypermanager.manager import HyperManager
from dotenv import load_dotenv
from pipeline.queries import fetch_txs
from pipeline.db import DatabaseConnection, write_events_to_timescale, get_max_block_number

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
    manager = HyperManager("https://holesky.hypersync.xyz")
    try:
        yield manager
    finally:
        await manager.close()


def get_transaction_hashes(db: DatabaseConnection) -> list[str]:
    """
    Query OpenedCommitmentStored table for transaction hashes after the last processed block
    """
    try:
        conn = db.get_connection()
        max_block = get_max_block_number(conn, "l1transactions")
        print(f'max block number: {max_block}')
        
        with conn.cursor() as cursor:
            query = """
                SELECT txnhash
                FROM openedcommitmentstored
                WHERE blocknumber > %s
            """
            cursor.execute(query, (max_block,))
            results = cursor.fetchall()

        return [row[0] for row in results] if results else []
    except Exception as e:
        logger.error(f"Error fetching transaction hashes: {e}", exc_info=True)
        return []


async def process_l1_transactions(db: DatabaseConnection, tx_hashes: list[str]):
    """
    Process L1 transactions and write to TimescaleDB
    """
    try:
        if not tx_hashes:
            logger.info("No new transaction hashes to process")
            return

        logger.info(f"Fetching {len(tx_hashes)} L1 transactions")

        # Fetch transactions using your existing fetch_txs function
        df: pl.DataFrame = await fetch_txs(tx_hashes, url='https://holesky.hypersync.xyz')

        if df is not None and not df.is_empty():
            logger.info(f"Fetched {len(df)} L1 transactions")

            # Write to TimescaleDB
            conn = db.get_connection()
            write_events_to_timescale(conn, df, "l1transactions")
            logger.info("Successfully wrote L1 transactions to database")
        else:
            logger.info("No new L1 transactions to write")

    except Exception as e:
        logger.error(f"Error processing L1 transactions: {e}", exc_info=True)


async def main():
    """
    Main function to continuously fetch and store L1 transaction data
    """
    logger.info("Starting L1 transactions TimescaleDB pipeline")

    db = None
    try:
        # Create database connection
        db = DatabaseConnection(DB_PARAMS)
        
        async with get_manager():
            while True:
                logger.info("Starting new fetch cycle")

                # 1. Get transaction hashes from OpenedCommitmentStored
                tx_hashes = get_transaction_hashes(db)

                # 2. Process and store L1 transactions
                await process_l1_transactions(db, tx_hashes)

                logger.info("Completed fetch cycle, waiting for next iteration")
                await asyncio.sleep(30)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal, closing connections...")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())

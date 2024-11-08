import logging
import polars as pl
from hypermanager.events import EventConfig
from hypermanager.manager import HyperManager
from typing import Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def fetch_event_for_config(
    manager: HyperManager, base_event_config: EventConfig, block_number: int
) -> Optional[pl.DataFrame]:
    """
    Fetch event logs for a single event configuration.

    Parameters:
    - base_event_config (EventConfig): The event configuration for which to fetch event logs.
    - block_number (int): The block number to start the query from

    Returns:
    - Optional[pl.DataFrame]: A Polars DataFrame containing the fetched event logs, or None if no events were found.
    """
    try:
        # Query events using the event configuration and return the result as a Polars DataFrame
        df: pl.DataFrame = await manager.execute_event_query(
            base_event_config, tx_data=True, from_block=block_number
        )

        if df.is_empty():
            print(f"No events found for {
                  base_event_config.name}, continuing...")
            return None

        print(f"Events found for {base_event_config.name}:")
        print(df.shape)
        return df

    except Exception as e:
        print(f"Error querying {base_event_config.name}: {e}")
        return None  # Return None in case of an error


async def fetch_txs(
    l1_tx_list: Union[str, list[str]], url: str = "https://holesky.hypersync.xyz"
) -> Optional[pl.DataFrame]:
    CHUNK_SIZE = 2500
    if not l1_tx_list:
        logger.info("No L1 transaction hashes to query.")
        return None
    if isinstance(l1_tx_list, str):
        l1_tx_list = [l1_tx_list]

    # Ensure all transactions start with "0x"
    l1_tx_list = [tx if tx.startswith("0x") else f"0x{
        tx}" for tx in l1_tx_list]

    manager = HyperManager(url=url)
    dataframes = []

    def chunked(iterable, n):
        for i in range(0, len(iterable), n):
            yield iterable[i: i + n]

    for chunk in chunked(l1_tx_list, CHUNK_SIZE):
        try:
            l1_txs_chunk = await manager.search_txs(txs=chunk)
            if l1_txs_chunk is not None and not l1_txs_chunk.is_empty():
                dataframes.append(l1_txs_chunk)
        except Exception as e:
            logger.error(
                f"Unexpected error while fetching L1 transactions for chunk: {
                    e}"
            )
            continue
    if not dataframes:
        logger.info("No L1 transactions found.")
        return None
    return pl.concat(dataframes)

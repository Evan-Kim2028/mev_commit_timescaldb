import psycopg
import logging

logger = logging.getLogger(__name__)


class MaterializedViewManager:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def check_tables_exist(self) -> bool:
        """Check if all required tables exist."""
        required_tables = [
            'unopenedcommitmentstored',
            'openedcommitmentstored',
            'commitmentprocessed',
            'l1transactions'
        ]

        try:
            with self.conn.cursor() as cur:
                for table in required_tables:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table,))
                    if not cur.fetchone()[0]:
                        logger.info(f"Table {table} does not exist yet")
                        return False
            return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            return False

    def create_preconf_txs_view(self) -> bool:
        try:
            if not self.check_tables_exist():
                logger.info(
                    "Not all required tables exist yet. Skipping materialized view creation.")
                return False

            self.conn.commit()
            self.conn.autocommit = True

            with self.conn.cursor() as cur:
                # Check if view already exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE schemaname = 'api' 
                        AND matviewname = 'preconf_txs'
                    )
                """)
                view_exists = cur.fetchone()[0]

                if view_exists:
                    # If view exists, ensure the index exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_indexes 
                            WHERE schemaname = 'api' 
                            AND tablename = 'preconf_txs'
                            AND indexname = 'preconf_txs_unique_idx'
                        )
                    """)
                    index_exists = cur.fetchone()[0]

                    if not index_exists:
                        # Create the missing index
                        cur.execute("""
                            CREATE UNIQUE INDEX preconf_txs_unique_idx 
                            ON api.preconf_txs(commitmentIndex, hash);
                        """)

                    return True

                # Create api schema if it doesn't exist
                cur.execute("CREATE SCHEMA IF NOT EXISTS api;")

                # Drop existing view if exists
                cur.execute(
                    "DROP MATERIALIZED VIEW IF EXISTS api.preconf_txs CASCADE;")

                # Corrected view creation query without TimescaleDB option
                query = """
                CREATE MATERIALIZED VIEW api.preconf_txs AS
                WITH 
                    encrypted_stores AS (
                        SELECT commitmentIndex, committer, commitmentDigest 
                        FROM public.unopenedcommitmentstored
                    ),
                    commit_stores AS (
                        SELECT * FROM public.openedcommitmentstored
                    ),
                    commits_processed AS (
                        SELECT commitmentIndex, isSlash 
                        FROM public.commitmentprocessed
                    ),
                    l1_transactions AS (
                        SELECT * FROM public.l1transactions
                    ),
                    commitments_intermediate AS (
                        SELECT 
                            es.commitmentIndex,
                            es.committer,
                            es.commitmentDigest,
                            '0x' || cs.txnHash AS txnHash,
                            cp.isSlash,
                            cs.blocknumber AS inc_block_number,
                            l1.hash,
                            l1.timestamp,
                            l1.extra_data AS builder_graffiti,
                            cs.bidder,
                            cs.bid,
                            cs.decayStartTimeStamp,
                            cs.decayEndTimeStamp,
                            cs.dispatchTimestamp
                        FROM encrypted_stores es
                        INNER JOIN commit_stores cs ON es.commitmentIndex = cs.commitmentIndex
                        INNER JOIN commits_processed cp ON es.commitmentIndex = cp.commitmentIndex
                        INNER JOIN l1_transactions l1 ON '0x' || cs.txnHash = l1.hash
                    ),
                    commitments_final AS (
                        SELECT 
                            *,
                            CAST(bid AS NUMERIC) / POWER(10, 18) AS bid_eth,
                            TO_TIMESTAMP(timestamp / 1000) AS date,
                            GREATEST(
                                CASE 
                                    WHEN (decayEndTimeStamp - dispatchTimestamp) = 0 THEN 0
                                    ELSE (decayEndTimeStamp - decayStartTimeStamp)::FLOAT / (decayEndTimeStamp - dispatchTimestamp)
                                END, 
                                0
                            ) AS decay_multiplier,
                            GREATEST(
                                CASE 
                                    WHEN (decayEndTimeStamp - dispatchTimestamp) = 0 THEN 0
                                    ELSE (decayEndTimeStamp - decayStartTimeStamp)::FLOAT / (decayEndTimeStamp - dispatchTimestamp)
                                END, 
                                0
                            ) * (CAST(bid AS NUMERIC) / POWER(10, 18)) AS decayed_bid_eth
                        FROM commitments_intermediate
                    )
                SELECT 
                    *
                FROM commitments_final
                WITH NO DATA;
                """
                cur.execute(query)

                # Create a unique index
                cur.execute("""
                    CREATE UNIQUE INDEX preconf_txs_unique_idx 
                    ON api.preconf_txs(commitmentIndex, hash);
                """)

                # Populate the view
                cur.execute("REFRESH MATERIALIZED VIEW api.preconf_txs;")

                logger.info(
                    "Successfully created preconf_txs materialized view")
                return True

        except Exception as e:
            logger.error(f"Error creating preconf_txs materialized view: {e}")
            return False
        finally:
            if self.conn.autocommit:
                self.conn.autocommit = False

    def refresh_materialized_views(self) -> None:
        """Refresh all materialized views."""
        try:
            with self.conn.cursor() as cur:
                # First check if view and index exist
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE schemaname = 'api' 
                        AND matviewname = 'preconf_txs'
                    )
                """)
                view_exists = cur.fetchone()[0]

                if view_exists:
                    # Ensure we're in autocommit mode for concurrent refresh
                    old_autocommit = self.conn.autocommit
                    self.conn.autocommit = True
                    try:
                        cur.execute(
                            "REFRESH MATERIALIZED VIEW CONCURRENTLY api.preconf_txs;")
                        logger.info(
                            "Successfully refreshed preconf_txs materialized view")
                    finally:
                        self.conn.autocommit = old_autocommit
                else:
                    logger.info(
                        "preconf_txs materialized view doesn't exist yet, skipping refresh")

        except Exception as e:
            logger.error(f"Error refreshing materialized views: {e}")

import psycopg
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class MaterializedViewManager:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def check_tables_exist(self) -> bool:
        """Check if all required tables and views exist."""
        required_tables = [
            'unopenedcommitmentstored',
            'commitmentprocessed',
            'l1transactions'
        ]
        try:
            with self.conn.cursor() as cur:
                # Check tables
                for table in required_tables:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' AND table_name = %s
                        )
                    """, (table,))
                    if not cur.fetchone()[0]:
                        logger.info(f"Table {table} does not exist yet")
                        return False

                # Check for materialized view specifically
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE schemaname = 'public' 
                        AND matviewname = 'openedcommitmentstoredall'
                    )
                """)
                if not cur.fetchone()[0]:
                    logger.info(
                        "Materialized view openedcommitmentstoredall does not exist yet")
                    return False

                return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            return False

    def create_preconf_txs_view(self) -> bool:

        try:
            if not self.verify_permissions():
                logger.error(
                    "Insufficient permissions to create materialized view")
                return False

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
                        SELECT * FROM public.openedcommitmentstoredall
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

    def refresh_preconf_txs_view(self) -> None:
        """Refresh the preconf_txs materialized view."""
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

    def refresh_openedcommitments_view(self) -> None:
        """Refresh the openedcommitmentstoredall materialized view."""
        try:
            with self.conn.cursor() as cur:
                # Check if materialized view exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE schemaname = 'public' 
                        AND matviewname = 'openedcommitmentstoredall'
                    )
                """)
                view_exists = cur.fetchone()[0]

                if view_exists:
                    # Ensure we're in autocommit mode for concurrent refresh
                    old_autocommit = self.conn.autocommit
                    self.conn.autocommit = True
                    try:
                        cur.execute(
                            "REFRESH MATERIALIZED VIEW CONCURRENTLY openedcommitmentstoredall;")
                        logger.info(
                            "Successfully refreshed openedcommitmentstoredall materialized view")
                    finally:
                        self.conn.autocommit = old_autocommit
                else:
                    logger.info(
                        "openedcommitmentstoredall materialized view doesn't exist yet, skipping refresh")

        except Exception as e:
            logger.error(
                f"Error refreshing openedcommitmentstoredall materialized view: {e}")

    def refresh_all_views(self) -> None:
        """Refresh all views and materialized views."""
        try:
            # First refresh the consolidated view
            self.refresh_openedcommitments_view()

            # Then refresh the materialized view that depends on it
            self.refresh_preconf_txs_view()

        except Exception as e:
            logger.error(f"Error refreshing views: {e}")

    def create_openedcommitments_consolidated_view(self) -> bool:
        """Create a consolidated materialized view for opened commitments combining both table versions."""
        try:
            # Check if both required tables exist
            with self.conn.cursor() as cur:
                for table in ['openedcommitmentstored', 'openedcommitmentstoredv2']:
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

            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                # Check if materialized view already exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_matviews 
                        WHERE schemaname = 'public' 
                        AND matviewname = 'openedcommitmentstoredall'
                    )
                """)
                view_exists = cur.fetchone()[0]

                if view_exists:
                    cur.execute(
                        "DROP MATERIALIZED VIEW IF EXISTS openedcommitmentstoredall CASCADE;")

                # Create the consolidated materialized view with DISTINCT ON
                query = """
                    CREATE MATERIALIZED VIEW openedcommitmentstoredall AS
                    SELECT DISTINCT ON (commitmentIndex, txnHash)
                        commitmentIndex,
                        txnHash,
                        blocknumber,
                        bidder,
                        bid,
                        decayStartTimeStamp,
                        decayEndTimeStamp,
                        dispatchTimestamp
                    FROM (
                        SELECT 
                            commitmentIndex,
                            txnHash,
                            blocknumber,
                            bidder,
                            bidamt as bid,
                            decayStartTimeStamp,
                            decayEndTimeStamp,
                            dispatchTimestamp
                        FROM openedcommitmentstoredv2
                        UNION ALL
                        SELECT 
                            commitmentIndex,
                            txnHash,
                            blocknumber,
                            bidder,
                            bid,
                            decayStartTimeStamp,
                            decayEndTimeStamp,
                            dispatchTimestamp
                        FROM openedcommitmentstored
                    ) combined
                    ORDER BY commitmentIndex, txnHash, blocknumber DESC;
                """
                cur.execute(query)

                # Create the unique index
                cur.execute("""
                    CREATE UNIQUE INDEX openedcommitmentstoredall_unique_idx 
                    ON openedcommitmentstoredall(commitmentIndex, blocknumber);
                """)

                # Initial refresh of the materialized view
                cur.execute(
                    "REFRESH MATERIALIZED VIEW openedcommitmentstoredall;")

                logger.info(
                    "Successfully created openedcommitmentstoredall materialized view")
                return True

        except Exception as e:
            logger.error(
                f"Error creating openedcommitmentstoredall materialized view: {e}")
            return False
        finally:
            if self.conn.autocommit:
                self.conn.autocommit = False

    def merge_staked_columns(self) -> None:
        """Merge txoriginator from staked_old into msgsender in staked based on valblspubkey."""
        try:
            with self.conn.cursor() as cur:
                # Check if the staked and staked_old tables exist
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'staked'
                    )
                """)
                staked_exists = cur.fetchone()[0]

                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'staked_old'
                    )
                """)
                staked_old_exists = cur.fetchone()[0]

                if not staked_exists or not staked_old_exists:
                    logger.info("staked or staked_old table does not exist.")
                    return

                # Check if there are any rows to update
                check_query = """
                SELECT COUNT(*) FROM staked
                JOIN staked_old ON staked.valblspubkey = staked_old.valblspubkey
                WHERE staked.msgsender <> staked_old.txoriginator
                  AND staked_old.txoriginator IS NOT NULL;
                """
                cur.execute(check_query)
                count = cur.fetchone()[0]

                if count > 0:
                    # Run the UPDATE query
                    update_query = """
                    UPDATE staked
                    SET msgsender = staked_old.txoriginator
                    FROM staked_old
                    WHERE staked.valblspubkey = staked_old.valblspubkey
                      AND staked_old.txoriginator IS NOT NULL
                      AND staked.msgsender <> staked_old.txoriginator;
                    """
                    cur.execute(update_query)
                    self.conn.commit()
                    logger.info(
                        f"Successfully merged staked tables, updated {count} rows.")
                else:
                    logger.info("No staked data to merge.")
        except Exception as e:
            logger.error(f"Error merging staked tables: {e}")
            self.conn.rollback()

    @contextmanager
    def autocommit(self):
        """Context manager for autocommit operations"""
        if self.conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS:
            self.conn.commit()  # Commit any pending transaction
        original_autocommit = self.conn.autocommit
        try:
            self.conn.autocommit = True
            yield self.conn
        finally:
            if not self.conn.closed:
                if self.conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS:
                    self.conn.commit()
                self.conn.autocommit = original_autocommit


def verify_permissions(self) -> bool:
    try:
        with self.conn.cursor() as cur:
            # Check schema creation permission
            cur.execute("CREATE SCHEMA IF NOT EXISTS api;")

            # Test materialized view creation permission
            cur.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS api.test_permissions 
                AS SELECT 1 AS col WITH NO DATA
            """)
            cur.execute(
                "DROP MATERIALIZED VIEW IF EXISTS api.test_permissions")
            return True
    except Exception as e:
        logger.error(f"Insufficient permissions: {e}")
        return False

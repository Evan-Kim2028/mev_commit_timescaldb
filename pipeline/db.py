import polars as pl
import psycopg
from typing import Dict, Tuple, List
from contextlib import contextmanager

RESERVED_KEYWORDS = {
    'window', 'user', 'order', 'group', 'default', 'check', 'index',
    'primary', 'foreign', 'references', 'constraint', 'select', 'where',
    'from', 'table', 'column', 'limit', 'to', 'into', 'update', 'delete',
    'insert', 'values', 'value', 'any', 'some', 'all', 'distinct', 'as',
    'between', 'by', 'join', 'cross', 'inner', 'outer', 'left', 'right',
    'natural', 'union', 'intersect', 'except', 'case', 'when', 'then', 'else',
    'end', 'desc', 'asc', 'nulls', 'first', 'last', 'grant', 'with'
}


def normalize_table_name(table_name: str) -> str:
    """
    Normalize table name to lowercase and handle any special characters.

    Args:
        table_name: The table name to normalize

    Returns:
        str: Normalized table name in lowercase
    """
    return table_name.lower().strip()


class DatabaseConnection:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)
        return self.conn

    def get_connection(self):
        return self.connect()

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    @contextmanager
    def transaction(self):
        """Context manager for transactions"""
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

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


def escape_column_name(name: str) -> str:
    """
    Escape column names that are PostgreSQL reserved keywords.
    """
    if name.lower() in RESERVED_KEYWORDS:
        return f'"{name}"'
    return name


def get_primary_key_columns(table_name: str) -> List[str]:
    """
    Return primary key columns based on the table name.
    """
    if table_name in ['staked', 'staked_old']:
        return ['block_number', 'valblspubkey']
    else:
        return ['block_number', 'hash']


def convert_schema_to_timescale(schema: Dict[str, pl.DataType]) -> list[Tuple[str, str]]:
    """
    Convert Polars schema to TimescaleDB column definitions.
    """
    def get_timescale_type(name: str, pl_dtype: pl.DataType) -> str:
        if name.lower() == 'block_number':
            return "BIGINT"
        elif name.lower() in ['hash', 'valblspubkey']:
            return "TEXT"
        if isinstance(pl_dtype, pl.Datetime):
            return "TIMESTAMPTZ"
        elif isinstance(pl_dtype, pl.Int64):
            return "BIGINT"
        elif isinstance(pl_dtype, pl.Float64):
            return "DOUBLE PRECISION"
        elif isinstance(pl_dtype, pl.Boolean):
            return "BOOLEAN"
        elif isinstance(pl_dtype, pl.UInt64):
            return "NUMERIC(78)"
        else:
            return "TEXT"

    return [(escape_column_name(name), get_timescale_type(name, dtype))
            for name, dtype in schema.items()]


def create_or_update_event_table(conn: psycopg.Connection, table_name: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return

    table_name = normalize_table_name(table_name)
    schema_columns = convert_schema_to_timescale(df.schema)

    primary_key_columns = get_primary_key_columns(table_name)
    missing_columns = [
        col for col in primary_key_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required primary key columns {
                         missing_columns} in table {table_name}")

    primary_key = ', '.join(escape_column_name(col)
                            for col in primary_key_columns)

    with conn.cursor() as cur:
        try:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            table_exists = cur.fetchone()[0]

            if not table_exists:
                # Create table based on DataFrame schema
                columns = [f"{name} {dtype}" for name, dtype in schema_columns]
                create_query = f"""
                    CREATE TABLE {table_name} (
                        {', '.join(columns)},
                        PRIMARY KEY ({primary_key})
                    )
                """
                cur.execute(create_query)

                # Convert to hypertable using block_number
                cur.execute(f"""
                    SELECT create_hypertable(
                        '{table_name}',
                        'block_number',
                        chunk_time_interval => 100000,
                        if_not_exists => TRUE
                    )
                """)
            else:
                # Get all existing columns and their types
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                existing_columns = {col[0].lower(): col[1]
                                    for col in cur.fetchall()}

                # Check for new columns and add them if necessary
                for name, dtype in schema_columns:
                    clean_name = name.replace('"', '').lower()
                    if clean_name not in existing_columns:
                        cur.execute(f"ALTER TABLE {
                                    table_name} ADD COLUMN {name} {dtype}")
                    # Optionally, check if the data type matches and alter if needed

            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e


def get_max_block_number(conn: psycopg.Connection, table_name: str) -> int:
    # Normalize table name
    table_name = normalize_table_name(table_name)

    with conn.cursor() as cur:
        # First check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))

        table_exists = cur.fetchone()[0]

        if not table_exists:
            return 0

        # If table exists, get max block number
        cur.execute(f"""
            SELECT COALESCE(MAX(block_number), 0)
            FROM {table_name}
        """)
        return cur.fetchone()[0]


def normalize_column_name(name: str) -> str:
    """
    Normalize column name to lowercase and handle special characters.

    Args:
        name: The column name to normalize

    Returns:
        str: Normalized column name
    """
    return name.lower().strip()


def write_events_to_timescale(conn: psycopg.Connection, df: pl.DataFrame, table_name: str) -> None:
    if df.is_empty():
        return

    table_name = normalize_table_name(table_name)

    try:
        # Normalize column names in the DataFrame
        df = df.rename({col: normalize_column_name(col) for col in df.columns})

        # Ensure required columns are correctly typed
        # First, ensure block_number is Int64
        df = df.with_columns(pl.col('block_number').cast(pl.Int64))

        # Get primary key columns
        primary_key_columns = get_primary_key_columns(table_name)

        # Ensure primary key columns are present
        missing_columns = [
            col for col in primary_key_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required primary key columns {
                             missing_columns} in table {table_name}")

        # Cast primary key columns to appropriate types
        for col in primary_key_columns:
            if col == 'block_number':
                df = df.with_columns(pl.col('block_number').cast(pl.Int64))
            elif col == 'hash':
                df = df.with_columns(pl.col('hash').cast(pl.Utf8))
            elif col == 'valblspubkey':
                df = df.with_columns(pl.col('valblspubkey').cast(pl.Utf8))

        create_or_update_event_table(conn, table_name, df)

        # Convert UInt64 columns to strings to handle large numbers
        schema = df.schema
        uint64_cols = [name for name, dtype in schema.items()
                       if isinstance(dtype, pl.UInt64) and name.lower() not in primary_key_columns]

        if uint64_cols:
            df = df.with_columns([
                pl.col(col).cast(pl.Utf8) for col in uint64_cols
            ])

        # Prepare column names and placeholders for the INSERT query
        columns = [escape_column_name(col) for col in df.columns]
        placeholders = ', '.join(['%s'] * len(columns))

        # Convert DataFrame to records
        records = [tuple(row) for row in df.rows()]

        conflict_columns = ', '.join(escape_column_name(col)
                                     for col in primary_key_columns)

        with conn.cursor() as cur:
            cur.executemany(f"""
                INSERT INTO {table_name}
                ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_columns}) DO NOTHING
            """, records)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

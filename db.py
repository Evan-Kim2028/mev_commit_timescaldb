import polars as pl
import psycopg
from typing import Dict, Tuple


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


def create_connection(db_params: dict) -> psycopg.Connection:
    try:
        return psycopg.connect(**db_params)
    except psycopg.Error as e:
        print(f"Error connecting to database: {e}")
        raise


def escape_column_name(name: str) -> str:
    """
    Escape column names that are PostgreSQL reserved keywords.
    """
    if name.lower() in RESERVED_KEYWORDS:
        return f'"{name}"'
    return name


def convert_schema_to_timescale(schema: Dict[str, pl.DataType]) -> list[Tuple[str, str]]:
    """
    Convert Polars schema to TimescaleDB column definitions.
    """
    def get_timescale_type(name: str, pl_dtype: pl.DataType) -> str:
        # Special handling for block_number
        if name.lower() == 'block_number':
            return "BIGINT"

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
                        PRIMARY KEY (block_number)
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
                    # Optionally, you could check if the data type matches and alter if needed
                    # else:
                    #     if existing_columns[clean_name].upper() != dtype.upper():
                    #         cur.execute(f"ALTER TABLE {table_name} ALTER COLUMN {name} TYPE {dtype}")

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

        # Ensure block_number is the correct type before creating/updating table
        if 'block_number' in df.columns:
            df = df.with_columns(pl.col('block_number').cast(pl.Int64))

        create_or_update_event_table(conn, table_name, df)

        # Convert UInt64 columns to strings to handle large numbers
        schema = df.schema
        uint64_cols = [name for name, dtype in schema.items()
                       if isinstance(dtype, pl.UInt64) and name.lower() != 'block_number']

        if uint64_cols:
            df = df.with_columns([
                pl.col(col).cast(pl.Utf8) for col in uint64_cols
            ])

        # Prepare column names and placeholders for the INSERT query
        columns = [escape_column_name(col) for col in df.columns]
        placeholders = ', '.join(['%s'] * len(columns))

        # Convert DataFrame to records
        records = [tuple(row) for row in df.rows()]

        with conn.cursor() as cur:
            cur.executemany(f"""
                INSERT INTO {table_name}
                ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (block_number) DO NOTHING
            """, records)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

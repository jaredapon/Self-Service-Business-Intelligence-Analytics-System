"""
Centralized I/O service for the data pipeline.

Handles all interactions with the SQL database and MinIO object storage.
"""
from __future__ import annotations
import uuid
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.error import S3Error
from sqlalchemy.engine import Engine
from sqlalchemy import text

from app.core.config import settings
from app.db.session import engine  # global Engine


# -------------------------
# MinIO client
# -------------------------
try:
    minio_client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access,
        secret_key=settings.minio_secret,
        secure=settings.minio_secure,
    )
    print("✅ Successfully connected to MinIO.")
except Exception as e:
    print(f"❌ Failed to connect to MinIO: {e}")
    minio_client = None


# -------------------------
# Helpers
# -------------------------
def _pg_ident(c: str) -> str:
    """
    Quote identifiers only when needed (spaces or mixed case).
    """
    needs_quotes = (' ' in c) or (c != c.lower())
    return f'"{c}"' if needs_quotes else c


def _execute_sql(sql_statement: str, engine_: Engine = engine):
    """Execute raw SQL inside a transaction."""
    with engine_.begin() as conn:
        conn.execute(text(sql_statement))


# -------------------------
# DB I/O
# -------------------------
def write_df_to_sql(
    df: pd.DataFrame,
    table_name: str,
    method: str = 'append',
    pkey_cols: list[str] | None = None,
    engine_: Engine = engine,
):
    """
    Writes a pandas DataFrame to a SQL table.

    - Columns MUST match DB schema exactly (including case/quotes for spaced names).
    - method: 'append' | 'upsert'
    """
    if df.empty:
        print(f"  DataFrame for table '{table_name}' is empty. Skipping write.")
        return

    if method not in ('append', 'upsert'):
        raise ValueError("method must be 'append' or 'upsert'")

    if method == 'append':
        print(f"  Appending {len(df)} rows to {table_name}...")
        with engine_.connect() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=1000)
        return

    # --- upsert path ---
    if not pkey_cols:
        raise ValueError("pkey_cols is required for upsert")

    temp_table_name = f"temp_{table_name}_{str(uuid.uuid4()).replace('-', '')}"

    # stage to temp table (exact column names)
    with engine_.connect() as conn:
        df.to_sql(temp_table_name, conn, if_exists='replace', index=False, chunksize=1000)

    cols_raw = list(df.columns)
    cols_ins = [_pg_ident(c) for c in cols_raw]
    pkeys = [_pg_ident(c) for c in pkey_cols]
    updatable = [c for c in cols_ins if c not in pkeys]

    update_clause = (
        "UPDATE SET " + ", ".join([f"{c} = EXCLUDED.{c}" for c in updatable])
        if updatable else "NOTHING"
    )

    insert_cols_clause = ", ".join(cols_ins)
    pkey_clause = ", ".join(pkeys)

    sql = f"""
    INSERT INTO {_pg_ident(table_name)} ({insert_cols_clause})
    SELECT {insert_cols_clause} FROM {_pg_ident(temp_table_name)}
    ON CONFLICT ({pkey_clause}) DO {update_clause};
    """

    print(f"  Upserting {len(df)} rows into {table_name}...")
    try:
        _execute_sql(sql, engine_)
    finally:
        _execute_sql(f'DROP TABLE IF EXISTS {_pg_ident(temp_table_name)};', engine_)


def read_sql_to_df(sql_query_or_table: str, engine_: Engine = engine) -> pd.DataFrame:
    """
    Reads data from a SQL table or query into a DataFrame.
    Accepts a full SELECT or a bare table name.
    """
    print(f"  Reading from SQL: {sql_query_or_table}...")
    try:
        with engine_.connect() as conn:
            is_table = sql_query_or_table.strip().upper().startswith("SELECT") is False \
                       and " " not in sql_query_or_table.strip()
            if is_table:
                df = pd.read_sql(f'SELECT * FROM {_pg_ident(sql_query_or_table)}', conn)
            else:
                df = pd.read_sql(sql_query_or_table, conn)
        print(f"  Successfully read {len(df)} rows.")
        return df
    except Exception as e:
        print(f"❌ Error reading from SQL {sql_query_or_table}: {e}")
        raise


# -------------------------
# MinIO I/O
# -------------------------
def get_csv_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    """Download a CSV from MinIO to a DataFrame."""
    if not minio_client:
        print(f"MinIO client not available. Cannot download {object_name}.")
        return pd.DataFrame()

    try:
        print(f"  Downloading from MinIO: {bucket}/{object_name}")
        response = minio_client.get_object(bucket, object_name)
        file_content = BytesIO(response.read())
        df = pd.read_csv(file_content)
        response.close()
        response.release_conn()
        return df
    except S3Error as e:
        print(f"Error getting file from MinIO at {bucket}/{object_name}: {e}")
        return pd.DataFrame()


def upload_df_to_minio(df: pd.DataFrame, bucket: str, object_name: str, index: bool = False):
    """Upload a DataFrame CSV to MinIO."""
    if not minio_client:
        print("MinIO client not available. Skipping upload.")
        return

    csv_bytes = df.to_csv(index=index).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    try:
        minio_client.put_object(
            bucket,
            object_name,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type='text/csv',  # fix content type
        )
        print(f"  Successfully uploaded to MinIO: {bucket}/{object_name}")
    except S3Error as e:
        print(f"Error uploading {object_name} to MinIO: {e}")

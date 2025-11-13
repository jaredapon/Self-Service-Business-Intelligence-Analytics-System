import os
import io
from typing import List, Dict, Any, Optional

import pandas as pd
from minio import Minio
import psycopg2
from psycopg2.extras import execute_values
from app.core.config import settings

from dotenv import load_dotenv
load_dotenv()

# =========================
# CONFIG
# =========================

MINIO_ENDPOINT = settings.minio_endpoint
MINIO_ACCESS_KEY = settings.minio_access
MINIO_SECRET_KEY = settings.minio_secret
MINIO_SECURE = settings.minio_secure

MINIO_LANDING_BUCKET = settings.minio_landing_bucket
RAW_SALES_PREFIX = settings.minio_raw_sales_folder
RAW_SALES_BY_PRODUCT_PREFIX = settings.minio_raw_sales_by_product_folder

# NEW: staging buffer
MINIO_STAGING_BUCKET = settings.minio_staging_bucket
MINIO_ETL_FOLDER = settings.minio_etl_folder

PG_HOST = settings.db_host
PG_PORT = settings.db_port
PG_DBNAME = settings.db_name
PG_USER = settings.db_user
PG_PASSWORD = settings.db_password


# =========================
# MINIO HELPERS
# =========================

def _get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

def _ensure_staging_bucket() -> None:
    """Create staging bucket if missing (idempotent)."""
    client = _get_minio_client()
    if not client.bucket_exists(MINIO_STAGING_BUCKET):
        client.make_bucket(MINIO_STAGING_BUCKET)

def staging_put_bytes(object_name: str, data: bytes, content_type: str = "text/csv") -> int:
    """Upload raw bytes to staging bucket under given object name."""
    _ensure_staging_bucket()
    client = _get_minio_client()
    bio = io.BytesIO(data)
    client.put_object(
        bucket_name=MINIO_STAGING_BUCKET,
        object_name=object_name,
        data=bio,
        length=len(data),
        content_type=content_type,
    )
    return len(data)

def staging_get_bytes(object_name: str) -> bytes:
    """Download object from staging bucket to bytes."""
    client = _get_minio_client()
    resp = client.get_object(MINIO_STAGING_BUCKET, object_name)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()

def staging_delete_prefix(prefix: str) -> None:
    """Delete all objects under a prefix in staging bucket."""
    client = _get_minio_client()
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    for obj in client.list_objects(MINIO_STAGING_BUCKET, prefix=prefix, recursive=True):
        client.remove_object(MINIO_STAGING_BUCKET, obj.object_name)


def _list_excel_objects(prefix: str) -> List[Dict[str, Any]]:
    """
    List *.xlsx objects under the given prefix in the landing bucket.
    """
    client = _get_minio_client()
    objects: List[Dict[str, Any]] = []
    for obj in client.list_objects(MINIO_LANDING_BUCKET, prefix=prefix, recursive=True):
        if obj.object_name.lower().endswith(".xlsx"):
            objects.append({"object_name": obj.object_name})
    return objects


def _load_excel_from_minio(object_name: str) -> io.BytesIO:
    """
    Load an Excel object from MinIO into memory as BytesIO.
    """
    client = _get_minio_client()
    response = client.get_object(MINIO_LANDING_BUCKET, object_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()
    return io.BytesIO(data)


def get_sales_files_from_minio() -> List[Dict[str, Any]]:
    """
    Return list of sales files:
    [{"object_name": str, "fileobj": BytesIO}, ...]
    from landing/raw_sales/.
    """
    files: List[Dict[str, Any]] = []
    for meta in _list_excel_objects(RAW_SALES_PREFIX):
        fileobj = _load_excel_from_minio(meta["object_name"])
        files.append({"object_name": meta["object_name"], "fileobj": fileobj})
    return files


def get_sales_by_product_files_from_minio() -> List[Dict[str, Any]]:
    """
    Return list of sales-by-product files:
    [{"object_name": str, "fileobj": BytesIO}, ...]
    from landing/raw_sales_by_product/.
    """
    files: List[Dict[str, Any]] = []
    for meta in _list_excel_objects(RAW_SALES_BY_PRODUCT_PREFIX):
        fileobj = _load_excel_from_minio(meta["object_name"])
        files.append({"object_name": meta["object_name"], "fileobj": fileobj})
    return files


# =========================
# POSTGRES HELPERS
# =========================

def _get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
    )

def copy_csv_bytes_to_table(csv_bytes: bytes, table_name: str, columns: List[str]) -> None:
    """
    Bulk load with COPY FROM STDIN (HEADER). We explicitly list columns to avoid
    reliance on physical column order.
    """
    collist = ", ".join(f'"{c}"' for c in columns)
    copy_sql = f"COPY {table_name} ({collist}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, io.StringIO(csv_bytes.decode("utf-8")))
        conn.commit()

def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    key_columns: Optional[List[str]] = None,
):
    """
    Legacy generic UPSERT (still here; not used by buffer flow).
    """
    if df is None or df.empty:
        return

    cols = list(df.columns)
    records = [tuple(row[c] for c in cols) for _, row in df.iterrows()]
    col_list = ", ".join(f'"{c}"' for c in cols)

    if key_columns:
        conflict_cols = ", ".join(f'"{c}"' for c in key_columns)
        update_cols = [c for c in cols if c not in key_columns]
        if update_cols:
            set_clause = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in update_cols
            )
            sql = f"""
                INSERT INTO {table_name} ({col_list})
                VALUES %s
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {set_clause}
            """
        else:
            sql = f"""
                INSERT INTO {table_name} ({col_list})
                VALUES %s
                ON CONFLICT ({conflict_cols})
                DO NOTHING
            """
    else:
        sql = f"""
            INSERT INTO {table_name} ({col_list})
            VALUES %s
        """

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, records, page_size=1000)
        conn.commit()


# =========================
# TABLE-SPECIFIC UPSERT WRAPPERS (legacy)
# =========================

def upsert_fact_transaction_dimension(df: pd.DataFrame):
    key_cols = [c for c in ["date", "receipt_no", "product_id", "time_id"] if c in df.columns]
    upsert_dataframe(df, "fact_transaction_dimension", key_cols)


def upsert_current_product_dimension(df: pd.DataFrame):
    key_cols = [c for c in ["product_id"] if c in df.columns]
    upsert_dataframe(df, "current_product_dimension", key_cols)


def upsert_history_product_dimension(df: pd.DataFrame):
    key_cols = [c for c in ["product_id", "record_version"] if c in df.columns]
    upsert_dataframe(df, "history_product_dimension", key_cols)


def upsert_transaction_records(df: pd.DataFrame):
    key_cols = [c for c in ["receipt_no"] if c in df.columns]
    upsert_dataframe(df, "transaction_records", key_cols)


def upsert_time_dimension(df: pd.DataFrame):
    key_cols = [c for c in ["time_id"] if c in df.columns]
    upsert_dataframe(df, "time_dimension", key_cols)


# =========================
# HISTORY HELPERS (for incremental SCD)
# =========================

def has_history_product_dimension() -> bool:
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'history_product_dimension'
                )
                """
            )
            exists = cur.fetchone()[0]
            if not exists:
                return False

            cur.execute("SELECT EXISTS (SELECT 1 FROM history_product_dimension LIMIT 1)")
            has_rows = cur.fetchone()[0]
            return bool(has_rows)


def fetch_history_for_products(product_ids: List[str]) -> pd.DataFrame:
    if not product_ids:
        return pd.DataFrame()

    with _get_pg_conn() as conn:
        query = """
            SELECT
                product_id,
                product_name,
                price,
                record_version,
                is_current,
                last_transaction_datetime,
                parent_sku,
                category,
                product_cost
            FROM history_product_dimension
            WHERE product_id = ANY(%s)
        """
        return pd.read_sql(query, conn, params=(product_ids,))


# =========================
# NEW: BULK LOAD ORCHESTRATION FROM STAGING/ETL
# =========================

def bulk_load_from_staging(prefix: str, plan: List[Dict[str, Any]]) -> None:
    """
    prefix: e.g. 'etl/20251112_210315/' (with or without trailing '/')
    plan: list of dicts with keys:
      - table: postgres table name
      - filename: object name under prefix
      - columns: ordered column list in the CSV
    """
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    for step in plan:
        obj_name = prefix + step["filename"]
        csv_bytes = staging_get_bytes(obj_name)
        copy_csv_bytes_to_table(csv_bytes, step["table"], step["columns"])


# =========================
# EXPORT DATA FROM POSTGRES (for model inputs)
# =========================

def export_table_to_csv(table_name: str) -> pd.DataFrame:
    """Export entire table from PostgreSQL to DataFrame."""
    with _get_pg_conn() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)


# =========================
# RESULT TABLES MANAGEMENT
# =========================

def clear_result_table(table_name: str) -> None:
    """Delete all data from a result table."""
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name}")
        conn.commit()


def load_result_csv_to_table(csv_bytes: bytes, table_name: str) -> None:
    """Load CSV bytes directly into result table (assumes columns match)."""
    # Read CSV to get column names
    import io
    df = pd.read_csv(io.BytesIO(csv_bytes))
    columns = list(df.columns)
    
    # Use COPY to load
    copy_csv_bytes_to_table(csv_bytes, table_name, columns)
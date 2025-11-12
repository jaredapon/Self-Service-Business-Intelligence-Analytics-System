import os
import io
from typing import List, Dict, Any, Optional

import pandas as pd
from minio import Minio
import psycopg2
from psycopg2.extras import execute_values


# =========================
# CONFIG
# =========================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

MINIO_LANDING_BUCKET = os.getenv("MINIO_LANDING_BUCKET", "landing")
RAW_SALES_PREFIX = os.getenv("MINIO_RAW_SALES_PREFIX", "raw_sales/")
RAW_SALES_BY_PRODUCT_PREFIX = os.getenv(
    "MINIO_RAW_SALES_BY_PRODUCT_PREFIX", "raw_sales_by_product/"
)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DBNAME = os.getenv("PG_DBNAME", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")


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


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    key_columns: Optional[List[str]] = None,
):
    """
    Generic UPSERT.
    - df: columns already in snake_case to match table.
    - key_columns: if provided, used in ON CONFLICT.
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
# TABLE-SPECIFIC UPSERT WRAPPERS
# =========================

def upsert_fact_transaction_dimension(df: pd.DataFrame):
    # Unique grain assumption: (date, receipt_no, product_id, time_id)
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
    """
    True if history_product_dimension exists and has at least one row.
    Used to choose between full build (initial load) vs incremental.
    """
    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            # Table exists?
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

            # Has data?
            cur.execute("SELECT EXISTS (SELECT 1 FROM history_product_dimension LIMIT 1)")
            has_rows = cur.fetchone()[0]
            return bool(has_rows)


def fetch_history_for_products(product_ids: List[str]) -> pd.DataFrame:
    """
    Fetch existing history rows for given product_ids from history_product_dimension.
    """
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
                last_transaction_date,
                parent_sku,
                category,
                product_cost
            FROM history_product_dimension
            WHERE product_id = ANY(%s)
        """
        return pd.read_sql(query, conn, params=(product_ids,))

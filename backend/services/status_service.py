"""
System status store (lightweight ops telemetry).

Schema:
  system_status(
      key         TEXT PRIMARY KEY,
      value       TEXT,
      updated_at  TIMESTAMP DEFAULT NOW()
  )

Typical uses:
  - Track ETL lifecycle ("etl_status": running/completed/error:msg)
  - Remember last processed object ("last_upload": uploads/..csv)
"""

from typing import Dict, List, Optional
from datetime import datetime
from db.pool import get_db_connection


def _ensure_table() -> None:
    """Create table if it doesn't exist."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS system_status (
              key TEXT PRIMARY KEY,
              value TEXT,
              updated_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        conn.commit()


def upsert(key: str, value: str) -> None:
    """
    Insert or update a status entry atomically.
    Examples:
        upsert("etl_status", "running")
        upsert("last_upload", "uploads/2025-10-08-100500.csv")
    """
    _ensure_table()
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO system_status (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = NOW();
            """,
            (key, value),
        )
        conn.commit()


def get(key: str) -> Optional[Dict[str, str]]:
    """
    Return a single status row as a dict or None if missing.
    Example:
        {"key":"etl_status","value":"completed","updated_at":"2025-10-08T10:05:00"}
    """
    _ensure_table()
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT key, value, updated_at FROM system_status WHERE key = %s;",
            (key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        k, v, ts = row
        return {"key": k, "value": v, "updated_at": _iso(ts)}


def snapshot() -> List[Dict[str, str]]:
    """
    Return all status rows (ordered by key).
    """
    _ensure_table()
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT key, value, updated_at FROM system_status ORDER BY key;")
        rows = cur.fetchall()
        return [{"key": k, "value": v, "updated_at": _iso(ts)} for k, v, ts in rows]


def delete(key: str) -> bool:
    """
    Delete a single status key. Returns True if a row was removed.
    """
    _ensure_table()
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM system_status WHERE key = %s;", (key,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted


def clear() -> int:
    """
    Truncate all status entries. Returns number of rows removed.
    Use carefully.
    """
    _ensure_table()
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM system_status;")
        (count_before,) = cur.fetchone()
        cur.execute("TRUNCATE TABLE system_status;")
        conn.commit()
        return int(count_before)


# ---- helpers ----

def _iso(ts: datetime) -> str:
    """Timestamp -> ISO string (no timezone conversion)."""
    return ts.isoformat() if isinstance(ts, datetime) else str(ts)

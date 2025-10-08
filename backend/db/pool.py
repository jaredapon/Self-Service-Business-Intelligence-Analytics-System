"""
This module initializes and manages a PostgreSQL connection pool.
It provides a context manager to safely get and return connections
from the pool, ensuring that connections are properly managed
throughout the application's lifecycle.
"""
import psycopg2.pool
from contextlib import contextmanager
from backend.core.config import settings

# Initialize the pool to None. It will be created by the application startup event.
pool = None

def init_pool():
    """Initializes the connection pool."""
    global pool
    pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port
    )

def close_pool():
    """Closes all connections in the pool."""
    global pool
    if pool:
        pool.closeall()

@contextmanager
def get_db_connection():
    """
    A context manager to get a connection from the pool.
    Usage:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    if not pool:
        raise ValueError("Connection pool is not initialized. Call init_pool() first.")
    
    conn = None
    try:
        conn = pool.getconn()
        yield conn
    finally:
        if conn:
            pool.putconn(conn)
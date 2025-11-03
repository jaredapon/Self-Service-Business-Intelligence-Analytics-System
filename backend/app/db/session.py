"""
Database session and engine setup.

Creates a pooled SQLAlchemy engine and a Session factory (SessionLocal),
and provides a FastAPI dependency `get_db` for request-scoped sessions.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from app.core.config import settings

# Create a pooled engine using settings from the environment
engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,                      # Validate connections before using them
    pool_recycle=settings.db_pool_recycle,   # Recycle to avoid stale connections
    pool_timeout=settings.db_pool_timeout,   # Wait time for a connection from the pool
)

# Session factory (synchronous)
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)

def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a DB session.

    Usage in routes:
        from fastapi import Depends
        from sqlalchemy.orm import Session
        from app.db.session import get_db

        def read_items(db: Session = Depends(get_db)):
            ...
    """
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager for a transactional session scope.

    Usage:
        with session_scope() as db:
            # do work
            db.add(obj)
            # commit happens automatically on success
    """
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def ping_db(timeout_seconds: int = 2) -> bool:
    """
    Lightweight connectivity check. Returns True if SELECT 1 succeeds.
    Useful for startup checks or diagnostics.
    """
    try:
        with engine.connect() as conn:
            conn.execution_options(timeout=timeout_seconds)
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
"""Utilities for working with the embedded DuckDB database."""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

_BASE_DIR = Path(os.getenv("DUCKDB_BASE_DIR", "data/cache"))
_BASE_DIR.mkdir(parents=True, exist_ok=True)
_DB_PATH = _BASE_DIR / os.getenv("DUCKDB_FILENAME", "sales.duckdb")
_SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

# The DuckDB Python API is thread-safe when each thread has its own connection.
# We keep a thread-local cache so background tasks can reuse connections safely.
_thread_local: threading.local = threading.local()


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create core tables if they do not exist."""
    if not _SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {_SCHEMA_PATH}")
    with _SCHEMA_PATH.open("r", encoding="utf-8") as handle:
        schema_sql = handle.read()
    conn.execute(schema_sql)


def get_connection(readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a thread-local DuckDB connection, creating it if necessary."""
    if readonly:
        conn = duckdb.connect(str(_DB_PATH))
        _ensure_schema(conn)
        return conn

    if getattr(_thread_local, "conn", None) is None:
        _thread_local.conn = duckdb.connect(str(_DB_PATH))
        _thread_local.conn.execute("PRAGMA threads=4")
        for stmt in ("INSTALL httpfs", "LOAD httpfs", "INSTALL parquet", "LOAD parquet"):
            try:
                _thread_local.conn.execute(stmt)
            except duckdb.IOException:
                logger.debug("DuckDB extension command failed: %s", stmt)
        _ensure_schema(_thread_local.conn)
    return _thread_local.conn


def reset_database() -> None:
    """Helper used in tests to recreate the database from scratch."""
    if _DB_PATH.exists():
        _DB_PATH.unlink()
    if getattr(_thread_local, "conn", None):
        _thread_local.conn.close()
        _thread_local.conn = None  # type: ignore[attr-defined]


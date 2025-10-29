from __future__ import annotations

import os
import time
from typing import Generator
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

from psycopg import Connection
from psycopg_pool import ConnectionPool, PoolTimeout

# ---------- env ----------
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Give yourself headroom for API + workers (adjust if DB max_connections is tight)
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "20"))     # total conns
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
DB_POOL_MAX_WAIT = float(os.getenv("DB_POOL_MAX_WAIT", "15"))   # seconds to wait for a conn
DB_OP_TIMEOUT    = float(os.getenv("DB_OP_TIMEOUT", "10"))      # server-side statement_timeout (seconds)

# Small retry window to ride out brief spikes
DB_BORROW_RETRIES = int(os.getenv("DB_BORROW_RETRIES", "1"))    # additional attempts after the first
DB_BORROW_SLEEP   = float(os.getenv("DB_BORROW_SLEEP", "0.15")) # seconds between retries

# ---------- helpers ----------
def _augment_conninfo(url: str) -> str:
    """
    Ensure useful defaults on the connection string, without clobbering explicit values.
    Adds: sslmode=require (if missing), connect_timeout, keepalives, statement_timeout.
    """
    if not url:
        raise RuntimeError("DATABASE_URL missing")

    p = urlsplit(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))

    # Conservative defaults; only set if absent
    q.setdefault("sslmode", "require")
    q.setdefault("connect_timeout", "5")
    q.setdefault("keepalives", "1")
    q.setdefault("keepalives_idle", "30")
    q.setdefault("keepalives_interval", "10")
    q.setdefault("keepalives_count", "5")

    # Server-side statement_timeout (milliseconds) via 'options'
    stmt_ms = str(int(DB_OP_TIMEOUT * 1000))
    options = q.get("options", "")
    if f"statement_timeout={stmt_ms}" not in options:
        extra = f"-c statement_timeout={stmt_ms}"
        options = f"{options} {extra}".strip() if options else extra
        q["options"] = options

    new_query = urlencode(q, doseq=True)
    return urlunsplit((p.scheme, p.netloc, p.path, new_query, p.fragment))

# Global pool (created on startup)
pool: ConnectionPool | None = None

def open_pool() -> None:
    """Create the global pool once per process."""
    global pool
    if pool is not None:
        return
    conninfo = _augment_conninfo(DATABASE_URL)
    pool = ConnectionPool(
        conninfo,
        max_size=DB_POOL_MAX_SIZE,
        min_size=DB_POOL_MIN_SIZE,
        timeout=DB_POOL_MAX_WAIT,  # wait this long for a free connection
        max_idle=30,               # recycle idle connections
        kwargs={"autocommit": False},
    )

def close_pool() -> None:
    """Close the global pool gracefully."""
    global pool
    if pool is not None:
        try:
            pool.close()
        finally:
            pool = None

def db_conn() -> Generator[Connection, None, None]:
    """
    FastAPI dependency: yields a pooled psycopg Connection.
    Commits on success, rolls back on exception.
    Retries briefly on PoolTimeout (configurable via env).
    """
    if pool is None:
        open_pool()
    assert pool is not None, "DB pool not initialized"

    attempt = 0
    while True:
        try:
            with pool.connection(timeout=DB_POOL_MAX_WAIT) as conn:
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            break
        except PoolTimeout:
            if attempt >= DB_BORROW_RETRIES:
                raise
            attempt += 1
            time.sleep(DB_BORROW_SLEEP)

# Optional alias for old imports
get_db = db_conn

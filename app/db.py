# backend/app/db.py
from __future__ import annotations

import os
from typing import Generator
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

from psycopg import Connection
from psycopg_pool import ConnectionPool

# ---------- env ----------
DATABASE_URL = os.getenv("DATABASE_URL", "")

DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))     # total conns
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_WAIT = float(os.getenv("DB_POOL_MAX_WAIT", "5"))    # seconds to wait for a conn
DB_OP_TIMEOUT    = float(os.getenv("DB_OP_TIMEOUT", "10"))      # per-connection statement_timeout (seconds)

# ---------- helpers ----------
def _augment_conninfo(url: str) -> str:
    """
    Ensure useful defaults on the connection string, without clobbering explicit values.
    Adds: sslmode=require (if missing), connect_timeout, keepalives, statement_timeout.
    """
    if not url:
        raise RuntimeError("DATABASE_URL missing")

    p = urlsplit(url)
    # Parse existing query params into a dict
    q = dict(parse_qsl(p.query, keep_blank_values=True))

    # Only set defaults if absent
    q.setdefault("sslmode", "require")                    # Render/Supabase prod best practice
    q.setdefault("connect_timeout", "5")                  # seconds
    q.setdefault("keepalives", "1")
    q.setdefault("keepalives_idle", "30")
    q.setdefault("keepalives_interval", "10")
    q.setdefault("keepalives_count", "5")

    # Apply a server-side statement timeout (milliseconds)
    # (Many Postgres providers honor 'options' with -c key=val)
    stmt_ms = str(int(DB_OP_TIMEOUT * 1000))
    options = q.get("options", "")
    if f"statement_timeout={stmt_ms}" not in options:
        extra = f"-c statement_timeout={stmt_ms}"
        options = f"{options} {extra}".strip() if options else extra
        q["options"] = options

    # Rebuild query string correctly
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
    # psycopg_pool uses 'max_size', 'min_size', and 'timeout' to wait for a connection
    pool = ConnectionPool(
        conninfo,
        max_size=DB_POOL_MAX_SIZE,
        min_size=DB_POOL_MIN_SIZE,
        timeout=DB_POOL_MAX_WAIT,   # how long to wait for a free connection
        max_idle=30,                # seconds to keep idle conns before recycling
        kwargs={"autocommit": False},  # we manage transactions explicitly
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
    Times out quickly if the pool is exhausted (DB_POOL_MAX_WAIT).
    """
    if pool is None:
        # Fallback: open the pool lazily if startup hook didn't run yet
        open_pool()
    assert pool is not None, "DB pool not initialized"

    # timeout here is how long to wait for a connection from the pool
    with pool.connection(timeout=DB_POOL_MAX_WAIT) as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


# Optional alias if some modules import get_db
get_db = db_conn

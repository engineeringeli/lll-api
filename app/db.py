# backend/app/db.py
from __future__ import annotations

import os
from typing import Generator
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

from psycopg import Connection
from psycopg.rows import dict_row  # <-- important
from psycopg_pool import ConnectionPool

# ---------- env ----------
DATABASE_URL = os.getenv("DATABASE_URL", "")

DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "15"))
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "2"))
DB_POOL_MAX_WAIT = float(os.getenv("DB_POOL_MAX_WAIT", "10"))   # seconds to wait for a conn
DB_OP_TIMEOUT    = float(os.getenv("DB_OP_TIMEOUT", "10"))      # server-side statement_timeout (s)

def _augment_conninfo(url: str) -> str:
    if not url:
        raise RuntimeError("DATABASE_URL missing")

    p = urlsplit(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))

    q.setdefault("sslmode", "require")
    q.setdefault("connect_timeout", "5")
    q.setdefault("keepalives", "1")
    q.setdefault("keepalives_idle", "30")
    q.setdefault("keepalives_interval", "10")
    q.setdefault("keepalives_count", "5")

    stmt_ms = str(int(DB_OP_TIMEOUT * 1000))
    options = q.get("options", "")
    if f"statement_timeout={stmt_ms}" not in options:
        extra = f"-c statement_timeout={stmt_ms}"
        options = f"{options} {extra}".strip() if options else extra
        q["options"] = options

    new_query = urlencode(q, doseq=True)
    return urlunsplit((p.scheme, p.netloc, p.path, new_query, p.fragment))

# Global pool
pool: ConnectionPool | None = None

def open_pool() -> None:
    global pool
    if pool is not None:
        return
    conninfo = _augment_conninfo(DATABASE_URL)
    pool = ConnectionPool(
        conninfo,
        max_size=DB_POOL_MAX_SIZE,
        min_size=DB_POOL_MIN_SIZE,
        timeout=DB_POOL_MAX_WAIT,  # wait for a free connection
        max_idle=30,               # recycle idle conns
        kwargs={"autocommit": False},  # we manage transactions
    )

def close_pool() -> None:
    global pool
    if pool is not None:
        try:
            pool.close()
        finally:
            pool = None

def db_conn() -> Generator[Connection, None, None]:
    """
    FastAPI dependency: yields a pooled psycopg Connection with dict rows.
    Commits on success, rolls back on exception.
    """
    if pool is None:
        open_pool()
    assert pool is not None, "DB pool not initialized"

    with pool.connection(timeout=DB_POOL_MAX_WAIT) as conn:
        # ensure all fetches return dicts like {"id": ..., ...}
        conn.row_factory = dict_row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

# Optional alias for older imports
get_db = db_conn

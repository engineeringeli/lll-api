# backend/app/deps.py
import os
import time
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

import redis
from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

log = logging.getLogger("deps")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

REDIS_URL = os.environ.get("REDIS_URL")

def _augment_conninfo(url: str) -> str:
    """
    Add safe defaults for hosted Postgres:
    - sslmode=require (unless already present)
    - TCP keepalives
    - connect_timeout
    """
    extras = {
        "sslmode": "require",
        "connect_timeout": "10",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "5",
    }
    if "?" in url:
        present = {
            kv.split("=", 1)[0].lower()
            for kv in url.split("?", 1)[1].split("&")
            if "=" in kv
        }
        suffix = "&".join(f"{k}={v}" for k, v in extras.items() if k not in present)
        return url + (("&" + suffix) if suffix else "")
    else:
        suffix = "&".join(f"{k}={v}" for k, v in extras.items())
        return url + ("?" + suffix)

CONNINFO = _augment_conninfo(DATABASE_URL)

_pool: Optional[ConnectionPool] = None

def _configure_connection(conn) -> None:
    """
    One-time per-connection tuning.
    - autocommit=True prevents sticky long transactions (PgBouncer-friendly)
    - try to disable prepared statements if supported by this psycopg build
    """
    if getattr(conn, "_lll_configured", False):
        return
    try:
        conn.autocommit = True
    except Exception as e:
        log.warning("Failed to set autocommit=True: %s", e)

    # Best-effort: some psycopg versions support this attribute.
    # If not present, skip silently (no DSN/URI params!).
    try:
        # psycopg3: prepare_threshold can be set on connection in some versions
        if hasattr(conn, "prepare_threshold"):
            conn.prepare_threshold = None  # disable server-prepared statements
    except Exception as e:
        log.debug("prepare_threshold not settable on this connection: %s", e)

    # If you want to also reduce statement cache size (another PgBouncer-safe tweak):
    try:
        if hasattr(conn, "statement_cache_size"):
            conn.statement_cache_size = 0
    except Exception as e:
        log.debug("statement_cache_size not settable: %s", e)

    setattr(conn, "_lll_configured", True)

def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    _pool = ConnectionPool(
        conninfo=CONNINFO,
        min_size=1,
        max_size=int(os.environ.get("DB_MAX_CONN", "10")),
        max_idle=30,
        max_lifetime=1800,
        timeout=int(os.environ.get("DB_POOL_TIMEOUT", "30")),
        kwargs={  # kwargs passed to psycopg.connect (NOT into the URI)
            "row_factory": dict_row,
        },
        open=False,
    )

    # open the pool and do a quick warmup
    try:
        _pool.open()
        with _pool.connection() as conn:
            _configure_connection(conn)
            with conn.cursor() as cur:
                cur.execute("select 1;")
                cur.fetchone()
        log.info("DB connectivity OK")
    except Exception as e:
        log.warning("Initial DB warmup failed (will retry on demand): %s", e)

    return _pool

@contextmanager
def _borrow():
    pool = _get_pool()
    last_err: Exception | None = None
    for attempt in range(2):
        try:
            with pool.connection() as conn:
                _configure_connection(conn)
                yield conn
            return
        except OperationalError as e:
            last_err = e
            log.warning("DB borrow OperationalError (attempt %d): %s", attempt + 1, e)
            time.sleep(0.2)
    assert last_err is not None
    raise last_err

def get_db() -> Iterator:
    with _borrow() as conn:
        yield conn

def get_redis():
    if not REDIS_URL:
        return None
    return redis.from_url(REDIS_URL, decode_responses=True)

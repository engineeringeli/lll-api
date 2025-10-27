# backend/app/deps.py
import os
import time
from contextlib import contextmanager
from typing import Iterator, Optional, Callable

import redis
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

# ----------- Environment -----------

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

REDIS_URL = os.environ.get("REDIS_URL")

DB_MAX_CONN = int(os.environ.get("DB_MAX_CONN", "5"))          # small, safe default
DB_POOL_TIMEOUT = int(os.environ.get("DB_POOL_TIMEOUT", "10")) # seconds
DB_MAX_IDLE = int(os.environ.get("DB_MAX_IDLE", "30"))         # seconds
DB_MAX_LIFETIME = int(os.environ.get("DB_MAX_LIFETIME", "1800"))  # seconds

# ----------- Conninfo helpers -----------

def _augment_conninfo(url: str) -> str:
    """
    Add safe defaults for hosted Postgres and proxies/pgbouncer:
    - sslmode=require
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

# ----------- Optional per-connection session setup -----------

def _configure(conn: Connection) -> None:
    """
    Runs each time a new physical connection is created by the pool.
    Keep these fast. Avoid BEGIN/transactions here.
    """
    # Short timeouts to prevent pool starvation from slow queries.
    # Values are in milliseconds.
    conn.execute("SET statement_timeout = 3000")
    conn.execute("SET idle_in_transaction_session_timeout = 3000")
    # Tag sessions in pg_stat_activity
    conn.execute("SET application_name = 'backend-api'")

# ----------- Global pool -----------

# Notes:
# - prepare_threshold=0 disables server-side prepared statements (pgbouncer-safe).
# - autocommit=True keeps you out of “idle in transaction”.
pool = ConnectionPool(
    conninfo=CONNINFO,
    min_size=1,
    max_size=DB_MAX_CONN,
    max_idle=DB_MAX_IDLE,
    max_lifetime=DB_MAX_LIFETIME,
    timeout=DB_POOL_TIMEOUT,
    open=True,  # open at import so first request doesn't pay pool startup cost
    configure=_configure,  # run once per new physical connection
    kwargs={
        "row_factory": dict_row,
        "autocommit": True,
        "prepare_threshold": 0,
        # Extra safety: enforce timeouts at connect time as well.
        "options": "-c statement_timeout=3000 -c idle_in_transaction_session_timeout=3000",
    },
)

# ----------- Borrow/return helpers -----------

@contextmanager
def _borrow(retries: int = 2, backoff_sec: float = 0.2) -> Iterator[Connection]:
    """
    Borrow a connection from the pool, with a small retry to smooth over
    brief spikes rather than immediately 500'ing with PoolTimeout.
    """
    last_err: Optional[Exception] = None
    for _ in range(max(1, retries)):
        try:
            with pool.connection() as conn:
                yield conn
            return
        except OperationalError as e:
            last_err = e
            time.sleep(backoff_sec)
    assert last_err is not None
    raise last_err

# ----------- FastAPI dependencies -----------

def get_db() -> Iterator[Connection]:
    """
    FastAPI dependency: yields a pooled connection. Autocommit is enabled,
    so each statement is its own transaction unless you explicitly manage one.
    """
    with _borrow() as conn:
        yield conn

def get_redis():
    if not REDIS_URL:
        return None
    # decode_responses=True => str in/out, not bytes
    return redis.from_url(REDIS_URL, decode_responses=True)

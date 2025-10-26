# backend/app/deps.py
import os
import time
from contextlib import contextmanager
from typing import Iterator

import redis
from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

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

pool = ConnectionPool(
    conninfo=CONNINFO,
    min_size=1,
    max_size=int(os.environ.get("DB_MAX_CONN", "3")),
    max_idle=30,
    max_lifetime=1800,
    timeout=int(os.environ.get("DB_POOL_TIMEOUT", "10")),
    kwargs={"row_factory": dict_row},
)

@contextmanager
def _borrow():
    last_err: Exception | None = None
    for _ in range(2):
        try:
            with pool.connection() as conn:
                yield conn
            return
        except OperationalError as e:
            last_err = e
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

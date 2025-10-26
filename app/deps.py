# backend/app/deps.py
import os
import time
from contextlib import contextmanager
from typing import Iterator

import redis
from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

# --- Env ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

REDIS_URL = os.environ.get("REDIS_URL")  # optional; needed if you use RQ
# NOTE: do NOT load_dotenv() in production on Render; set env vars in the dashboard.

def _augment_conninfo(url: str) -> str:
    """
    Ensure sensible defaults for hosted Postgres:
      - sslmode=require (unless already present)
      - TCP keepalives to survive NAT/idle timeouts
      - connect_timeout to fail fast on bad URLs
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
        existing = {kv.split("=", 1)[0].lower() for kv in url.split("?", 1)[1].split("&") if "=" in kv}
        suffix = "&".join(f"{k}={v}" for k, v in extras.items() if k.lower() not in existing)
        return url + (("&" + suffix) if suffix else "")
    else:
        suffix = "&".join(f"{k}={v}" for k in extras)
        return url + ("?" + suffix)

CONNINFO = _augment_conninfo(DATABASE_URL)

# --- Small pool (free tiers: keep it tiny) ---
pool = ConnectionPool(
    conninfo=CONNINFO,
    min_size=1,
    max_size=int(os.environ.get("DB_MAX_CONN", "3")),
    max_idle=30,         # seconds an idle conn can live in pool
    max_lifetime=1800,   # recycle long-lived connections (30 min)
    timeout=int(os.environ.get("DB_POOL_TIMEOUT", "10")),  # wait for a free conn
    kwargs={"row_factory": dict_row},  # return dict-like rows
)

@contextmanager
def _borrow():
    """
    Borrow a connection with a tiny retry to survive “server closed connection”
    right when we try to use it.
    """
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
        # You can raise here if your app requires Redis to boot:
        # raise RuntimeError("REDIS_URL not set")
        return None
    return redis.from_url(REDIS_URL, decode_responses=True)

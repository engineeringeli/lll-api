# backend/app/db.py
import os, time
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

# Hardened conninfo (add ssl + keepalives + sane timeouts)
def augment(url: str) -> str:
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    p = urlsplit(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.setdefault("sslmode", "require")
    q.setdefault("connect_timeout", "10")
    q.setdefault("keepalives", "1")
    q.setdefault("keepalives_idle", "30")
    q.setdefault("keepalives_interval", "10")
    q.setdefault("keepalives_count", "5")
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q.items()), p.fragment))

CONNINFO = augment(DATABASE_URL)

# Tune these by env so you can tweak without redeploying
POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))       # try 10–20 per API pod
POOL_MAX_WAIT = float(os.getenv("DB_POOL_MAX_WAIT", "5"))      # seconds to wait before timeout (fail fast)
POOL_TIMEOUT  = float(os.getenv("DB_OP_TIMEOUT", "25"))        # statement_timeout-ish at app layer

pool = ConnectionPool(
    conninfo=CONNINFO,
    max_size=POOL_MAX_SIZE,
    max_wait=POOL_MAX_WAIT,
    kwargs={"autocommit": False, "row_factory": dict_row},
)

# FastAPI lifecycle hooks (import this module in your app __init__)
def open_pool():
    pool.open()

def close_pool():
    pool.close()

@asynccontextmanager
async def db_conn():
    # Fail fast if all connections are busy
    with pool.connection(timeout=POOL_MAX_WAIT) as conn:
        with conn.cursor() as cur:
            # Optional: enforce statement_timeout per session (server-side)
            cur.execute("SET LOCAL statement_timeout = %s;", (int(POOL_TIMEOUT * 1000),))
        yield conn

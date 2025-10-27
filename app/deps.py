# backend/app/deps.py
import os
import time
import socket
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

import redis
from psycopg.rows import dict_row
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

log = logging.getLogger("deps")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ──────────────────────────────────────────────────────────────────────────────
# ENV
# ──────────────────────────────────────────────────────────────────────────────
RAW_DATABASE_URL = os.environ.get("DATABASE_URL")
if not RAW_DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

REDIS_URL = os.environ.get("REDIS_URL")

# If you want to test direct Postgres (5432) vs PgBouncer pooler (6543), set:
#   DB_FORCE_DIRECT=true  -> force 5432 (session pooling / full features)
#   DB_FORCE_POOLER=true  -> force 6543 (transaction pooling / requires prepare_threshold=None)
FORCE_DIRECT = os.getenv("DB_FORCE_DIRECT", "").lower() in ("1", "true", "yes")
FORCE_POOLER = os.getenv("DB_FORCE_POOLER", "").lower() in ("1", "true", "yes")

POOL_MAX = int(os.environ.get("DB_MAX_CONN", "3"))
POOL_TIMEOUT = int(os.environ.get("DB_POOL_TIMEOUT", "10"))

# ──────────────────────────────────────────────────────────────────────────────
# Conninfo utilities
# ──────────────────────────────────────────────────────────────────────────────
def _augment_conninfo(url: str) -> str:
    """
    Add safe defaults for hosted Postgres:
    - sslmode=require (unless already present)
    - connect_timeout, TCP keepalives (help across Render/Supabase/Cloudflare)
    - prepare_threshold=None (disable server-prepared statements for PgBouncer)
    """
    extras = {
        "sslmode": "require",
        "connect_timeout": "10",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "5",
        # psycopg3 — disable server-prepared statements so PgBouncer (6543) is happy
        # (safe on direct too)
        "prepare_threshold": "none",
        # helps some providers pick a writable primary
        "target_session_attrs": "read-write",
    }

    if "?" in url:
        base, qs = url.split("?", 1)
        seen = {kv.split("=", 1)[0].lower() for kv in qs.split("&") if "=" in kv}
        suffix = "&".join(f"{k}={v}" for k, v in extras.items() if k not in seen)
        return url + (("&" + suffix) if suffix else "")
    else:
        suffix = "&".join(f"{k}={v}" for k, v in extras.items())
        return url + "?" + suffix

def _override_host_port(url: str) -> str:
    """
    Optionally force pooler (6543) or direct (5432) without making you change the whole URL.
    Only touches the port/host pattern we expect from Supabase; otherwise returns url as-is.
    """
    if not (FORCE_DIRECT or FORCE_POOLER):
        return url

    # Very light-weight parse; works for standard postgres URLs.
    # e.g. postgresql://user:pass@host:port/db?...
    try:
        head, rest = url.split("://", 1)
        creds_and_host, tail = rest.split("/", 1)  # creds@host:port/db?...
        if "@" in creds_and_host:
            creds, hostport = creds_and_host.split("@", 1)
            userinfo = creds + "@"
        else:
            userinfo = ""
            hostport = creds_and_host

        if ":" in hostport:
            host, port = hostport.split(":", 1)
        else:
            host, port = hostport, ""

        # Supabase pooler host typically contains ".pooler."
        if FORCE_POOLER:
            # force pooler port
            if port != "6543":
                port = "6543"
            # keep host (if user provided already pooler host that's fine)
        elif FORCE_DIRECT:
            if port != "5432":
                port = "5432"

        new_hostport = f"{host}:{port}" if port else host
        new_rest = userinfo + new_hostport + "/" + tail
        return head + "://" + new_rest
    except Exception:
        # If parsing fails, don't mutate.
        return url

def _hostport_for_log(url: str) -> str:
    try:
        head, rest = url.split("://", 1)
        creds_and_host, tail = rest.split("/", 1)
        hostport = creds_and_host.split("@", 1)[-1]
        host = hostport.split(":", 1)[0]
        port = hostport.split(":", 1)[1] if ":" in hostport else "(default)"
        dbname = tail.split("?", 1)[0]
        return f"{host}:{port}/{dbname}"
    except Exception:
        return "(unable to parse host/port)"

BASE_CONNINFO = _augment_conninfo(_override_host_port(RAW_DATABASE_URL))
log.info("DB conn target: %s", _hostport_for_log(BASE_CONNINFO))

# ──────────────────────────────────────────────────────────────────────────────
# Lazy pool (don’t open a socket at import time)
# ──────────────────────────────────────────────────────────────────────────────
_pool: Optional[ConnectionPool] = None

def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    # Note: kwargs flow to psycopg.connect()
    _pool = ConnectionPool(
        conninfo=BASE_CONNINFO,
        min_size=1,                           # keep it small on free tiers
        max_size=POOL_MAX,
        max_idle=30,                          # seconds
        max_lifetime=1800,                    # seconds
        timeout=POOL_TIMEOUT,                 # seconds waiting for a slot
        kwargs={
            # row factory so rows are dict-like
            "row_factory": dict_row,
            # already requested via conninfo, but set here too (harmless on direct)
            "prepare_threshold": None,
            # optional: psycopg3 setting to be friendlier for short-lived queries
            # "autocommit": True,  # uncomment if you never use explicit transactions
        },
        open=False,  # lazy-open: first borrow actually connects
    )

    try:
        # Light warmup: open once so we fail fast if the URL is wrong.
        _pool.open()
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1;")
                cur.fetchone()
        log.info("DB connectivity OK")
    except Exception as e:
        log.warning("Initial DB warmup failed (will retry on demand): %s", e)

    return _pool

# ──────────────────────────────────────────────────────────────────────────────
# Borrow with one quick retry on transient errors
# ──────────────────────────────────────────────────────────────────────────────
@contextmanager
def _borrow():
    last_err: Exception | None = None
    pool = _get_pool()

    for attempt in (1, 2):
        try:
            with pool.connection() as conn:
                yield conn
            return
        except OperationalError as e:
            last_err = e
            log.warning("DB borrow OperationalError (attempt %d): %s", attempt, e)
            # brief backoff then try to reopen the pool and retry once
            time.sleep(0.3)
            try:
                pool.close(soft=False)
            except Exception:
                pass
            try:
                pool.open()
            except Exception as oe:
                last_err = oe
        except Exception as e:
            last_err = e
            break

    assert last_err is not None
    raise last_err

# ──────────────────────────────────────────────────────────────────────────────
# Public dependencies
# ──────────────────────────────────────────────────────────────────────────────
def get_db() -> Iterator:
    """
    FastAPI dependency that yields a psycopg3 connection from the pool.
    """
    with _borrow() as conn:
        yield conn

def get_redis():
    if not REDIS_URL:
        return None
    return redis.from_url(REDIS_URL, decode_responses=True)

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Depends, Body, HTTPException
from starlette.middleware.cors import CORSMiddleware

# DB & pooling
from psycopg import Connection
import psycopg
import redis as _redis  # for health_redis()

from app.db import db_conn, open_pool, close_pool, pool  # pooled helpers

# Queues / routes
from app.routes_settings import router as org_router
from app.routes_messages import router as msgs_router
from app.routes_webhooks import router as webhooks_router
from app.routes_leads import router as leads_router
from app.routes_docs import router as docs_router
from app.routes_contacts import router as contacts_router
from app.routes_debug import router as debug_router

try:
    from app.queue import get_queue
except ModuleNotFoundError:
    from .queue import get_queue

app = FastAPI(title="Lawyer Follow-up API")

# =========================
# CORS
# =========================
ALLOWED_ORIGIN_REGEX = r"^https://([a-z0-9-]+\.)?legalleadliaison\.com$"
ALLOW_LOCALHOST = os.getenv("ALLOW_LOCALHOST", "false").lower() == "true"
if ALLOW_LOCALHOST:
    allowed_regex = r"^(https://([a-z0-9-]+\.)?legalleadliaison\.com|http://localhost:3000)$"
else:
    allowed_regex = ALLOWED_ORIGIN_REGEX

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=allowed_regex,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# =========================
# Lifecycle
# =========================
@app.on_event("startup")
def _startup() -> None:
    open_pool()  # create pool once

@app.on_event("shutdown")
def _shutdown() -> None:
    close_pool()

# =========================
# Utilities
# =========================
def _mask_url(url: str) -> str:
    if not url:
        return ""
    try:
        if "://" in url and "@" in url:
            scheme, rest = url.split("://", 1)
            userinfo, host = rest.split("@", 1)
            if ":" in userinfo:
                return f"{scheme}://****:****@{host}"
            return f"{scheme}://****@{host}"
        return url
    except Exception:
        return "<masked>"

# =========================
# Health endpoints
# =========================
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/health/env")
def health_env():
    keys = [
        "DATABASE_URL",
        "REDIS_URL",
        "INLINE_APPROVE_SEND",
        "DISABLE_QUEUE",
        "ALLOW_LOCALHOST",
        "PORTAL_BASE",
        "UPLOADS_BUCKET",
        "DB_POOL_MAX_SIZE",
        "DB_POOL_MIN_SIZE",
        "DB_POOL_MAX_WAIT",
        "DB_OP_TIMEOUT",
        "DB_BORROW_RETRIES",
        "DB_BORROW_SLEEP",
    ]
    out: dict[str, Any] = {}
    for k in keys:
        v = os.getenv(k, "")
        out[k] = _mask_url(v) if "URL" in k else (v if v else "")
    return {"env": out}

@app.get("/health/redis")
def health_redis():
    url = os.getenv("REDIS_URL", "")
    if not url:
        return {"ok": False, "error": "REDIS_URL missing"}
    try:
        r = _redis.from_url(url, decode_responses=True)
        ok = r.ping()
        q_key = "rq:queue:outbound"
        q_len = r.llen(q_key) if r.exists(q_key) else 0
        workers = r.smembers("rq:workers") if r.exists("rq:workers") else set()
        return {
            "ok": bool(ok),
            "redis_url": _mask_url(url),
            "rq_queue_outbound_len": q_len,
            "rq_workers_seen": sorted(list(workers))[:10],
        }
    except Exception as e:
        return {"ok": False, "redis_url": _mask_url(url), "error": str(e)}

@app.get("/health/db")
def health_db():
    try:
        if pool is None:
            open_pool()
        assert pool is not None
        with pool.connection(timeout=float(os.getenv("DB_POOL_MAX_WAIT", "15"))) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                row = cur.fetchone()
        return {"ok": row == (1,), "via": "pool"}
    except Exception as e:
        url = os.getenv("DATABASE_URL", "")
        try:
            conn = psycopg.connect(url, connect_timeout=5)
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    row2 = cur.fetchone()
            return {"ok": row2 == (1,), "via": "direct", "database_url": _mask_url(url)}
        except Exception as e2:
            return {
                "ok": False,
                "via": "pool+direct-failed",
                "database_url": _mask_url(url),
                "error": f"{type(e).__name__}: {e}",
                "direct_error": f"{type(e2).__name__}: {e2}",
            }

# =========================
# Routers
# =========================
app.include_router(debug_router)
app.include_router(contacts_router)
app.include_router(org_router)
app.include_router(msgs_router)
app.include_router(webhooks_router)
app.include_router(leads_router)
app.include_router(docs_router)

# =========================
# Example endpoints (use pooled DB + queue)
# =========================
@app.get("/contacts")
def contacts(conn: Connection = Depends(db_conn)):
    """Return newest contacts (id, name, email, phone, status)."""
    try:
        rows = conn.execute(
            """
            SELECT id, first_name, last_name, email, phone, status
            FROM contacts
            ORDER BY created_at DESC
            LIMIT 50;
            """
        ).fetchall()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB query failed: {e}")

@app.post("/messages/send-email")
def api_send_email(
    to_email: str = Body(..., embed=True),
    subject: str = Body(..., embed=True),
    body_text: str = Body(..., embed=True),
):
    """Enqueue an email send via RQ worker (app.jobs.send_email)."""
    try:
        q = get_queue()
        job = q.enqueue("app.jobs.send_email", to_email, subject, body_text)
        return {"enqueued": True, "job_id": job.id}
    except Exception as e:
        raise HTTPException(500, f"enqueue failed: {e}")

@app.post("/messages/send-sms")
def api_send_sms(
    to_number: str = Body(..., embed=True),
    body_text: str = Body(..., embed=True),
):
    """Enqueue an SMS send via RQ worker (app.jobs.send_sms)."""
    try:
        q = get_queue()
        job = q.enqueue("app.jobs.send_sms", to_number, body_text)
        return {"enqueued": True, "job_id": job.id}
    except Exception as e:
        raise HTTPException(500, f"enqueue failed: {e}")

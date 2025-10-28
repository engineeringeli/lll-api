# backend/app/main.py

from fastapi import FastAPI, Depends, Body, HTTPException
from starlette.middleware.cors import CORSMiddleware
from psycopg import Connection

from app.deps import get_db
from app.routes_settings import router as org_router
from app.routes_messages import router as msgs_router
from app.routes_webhooks import router as webhooks_router
from app.routes_leads import router as leads_router
from app.routes_docs import router as docs_router
from app.routes_contacts import router as contacts_router
# backend/app/main.py (or wherever you include routers)
from app.routes_debug import router as debug_router
app.include_router(debug_router)


# prefer absolute import; fall back to relative if needed
try:
    from app.queue import get_queue
except ModuleNotFoundError:
    from .queue import get_queue

import os

app = FastAPI(title="Lawyer Follow-up API")

# --- CORS ---
# Matches:
#   https://app.legalleadliaison.com
#   https://acme.legalleadliaison.com
#   https://firm-123.legalleadliaison.com
ALLOWED_ORIGIN_REGEX = r"^https://([a-z0-9-]+\.)?legalleadliaison\.com$"

# Allow localhost in development only (optional)
ALLOW_LOCALHOST = os.getenv("ALLOW_LOCALHOST", "false").lower() == "true"
if ALLOW_LOCALHOST:
    # combine regexes: production domains OR http://localhost:3000
    allowed_regex = r"^(https://([a-z0-9-]+\.)?legalleadliaison\.com|http://localhost:3000)$"
else:
    allowed_regex = ALLOWED_ORIGIN_REGEX

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=allowed_regex,
    allow_credentials=True,  # required if you use cookies/sessions
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# ---------- health ----------
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}

def _mask_url(url: str) -> str:
    if not url:
        return ""
    try:
        # rediss://:password@host:port
        if "://" in url and "@" in url:
            scheme, rest = url.split("://", 1)
            if "@" in rest:
                _, host = rest.split("@", 1)
                return f"{scheme}://****:****@{host}"
        return url
    except Exception:
        return "<masked>"

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
    ]
    out = {}
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
        r = redis.from_url(url, decode_responses=True)
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
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return {"ok": False, "error": "DATABASE_URL missing"}
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        with conn:
            with conn.cursor() as cur:
                cur.execute("select 1;")
                row = cur.fetchone()
        return {"ok": row == (1,), "database_url": _mask_url(url)}
    except Exception as e:
        return {"ok": False, "database_url": _mask_url(url), "error": str(e)}

# ---------- include routers ----------
app.include_router(contacts_router)
app.include_router(org_router)
app.include_router(msgs_router)
app.include_router(webhooks_router)
app.include_router(leads_router)
app.include_router(docs_router)

# ---------- contacts (list) ----------
@app.get("/contacts")
def contacts(db: Connection = Depends(get_db)):
    """
    Returns the newest contacts (id, name, email, phone, status).
    """
    try:
        rows = db.execute(
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

# ---------- outbound: enqueue email ----------
@app.post("/messages/send-email")
def api_send_email(
    to_email: str = Body(..., embed=True),
    subject: str = Body(..., embed=True),
    body_text: str = Body(..., embed=True),
):
    """
    Enqueues an email send via RQ worker (app.jobs.send_email).
    """
    try:
        q = get_queue()
        job = q.enqueue("app.jobs.send_email", to_email, subject, body_text)
        return {"enqueued": True, "job_id": job.id}
    except Exception as e:
        raise HTTPException(500, f"enqueue failed: {e}")

# ---------- outbound: enqueue sms ----------
@app.post("/messages/send-sms")
def api_send_sms(
    to_number: str = Body(..., embed=True),
    body_text: str = Body(..., embed=True),
):
    """
    Enqueues an SMS send via RQ worker (app.jobs.send_sms).
    """
    try:
        q = get_queue()
        job = q.enqueue("app.jobs.send_sms", to_number, body_text)
        return {"enqueued": True, "job_id": job.id}
    except Exception as e:
        raise HTTPException(500, f"enqueue failed: {e}")

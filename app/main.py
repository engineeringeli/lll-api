# backend/app/main.py

from fastapi import FastAPI, Depends, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg import Connection
from app.deps import get_db
from app.routes_settings import router as org_router
from app.routes_messages import router as msgs_router
from app.routes_webhooks import router as webhooks_router
from app.routes_leads import router as leads_router
from app.routes_docs import router as docs_router
from app.routes_contacts import router as contacts_router
# prefer absolute import; fall back to relative if needed
try:
    from app.queue import get_queue
except ModuleNotFoundError:
    from .queue import get_queue

app = FastAPI(title="Lawyer Follow-up API")
app.include_router(contacts_router)
app.include_router(org_router)
app.include_router(msgs_router)
app.include_router(webhooks_router)
app.include_router(leads_router)
app.include_router(docs_router)

# --- CORS: allow your local frontend (Next.js on port 3000) ---
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- health ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/debug/redis")
def debug_redis():
    try:
        q = get_queue()
        return {"ok": bool(q.connection.ping())}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
        # surface a clean 500 to the client; details will be in server logs
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

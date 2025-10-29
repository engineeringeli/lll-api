# backend/app/routes_messages.py

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import os
import re
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Body, Query
from psycopg import Connection
from psycopg.types.json import Json

from app.deps import db_conn
from app.decisions import should_autosend
from app.followups import generate_initial_docs_request, _portal_url as build_portal_url
from app.queue import get_queue
# If you set INLINE_APPROVE_SEND=0, we’ll enqueue only; otherwise we may inline-fallback.
from app.jobs import send_message_and_update  # noqa: F401  (used for inline fallback)

router = APIRouter(prefix="/messages", tags=["messages"])

# Toggle: attempt inline send on approve if enqueue fails (defaults ON for reliability)
INLINE_APPROVE_SEND = os.getenv("INLINE_APPROVE_SEND", "1").lower() in {"1", "true", "yes", "on"}


# -----------------------------
# Helpers
# -----------------------------

def _org_settings(db: Connection) -> dict:
    row = db.execute("SELECT * FROM org_settings LIMIT 1;").fetchone()
    if not row:
        # sensible defaults if org_settings row not present yet
        return {
            "require_approval_initial": True,
            "autosend_confidence_threshold": 0.85,
            "business_hours_tz": "America/Los_Angeles",
            "business_hours_start": 8,
            "business_hours_end": 18,
            "cooldown_hours": 22,
            "max_daily_sends": 2,
            "grace_minutes": 5,
            # optional appearance fields (guarded below)
            "include_signature": True,
            "outbound_from_name": "",
            "outbound_signature": "",
        }
    return dict(row)


def _signature_block(db: Connection) -> str:
    row = db.execute(
        """
        SELECT
          COALESCE(include_signature, true) AS include_signature,
          outbound_from_name,
          outbound_signature
        FROM org_settings
        LIMIT 1;
        """
    ).fetchone() or {}
    if not row or not row.get("include_signature"):
        return ""
    name = (row.get("outbound_from_name") or "").strip()
    sig = (row.get("outbound_signature") or "").strip()
    parts = [p for p in (name, sig) if p]
    return "\n".join(parts)


def _finalize_body(db: Connection, body: str) -> str:
    # strip any bracketed notes e.g. [prompt tags]
    body = re.sub(r"\[[^\]]+\]", "", body or "").strip()
    sig = _signature_block(db)
    return f"{body}\n\n{sig}".strip() if sig else body


def _missing_labels(db: Connection, contact_id: str) -> list[str]:
    rows = db.execute(
        """
        SELECT dr.label
          FROM client_documents cd
          JOIN document_requirements dr ON dr.id = cd.requirement_id
         WHERE cd.contact_id = %s
           AND COALESCE(cd.is_required, dr.is_required) = TRUE
           AND cd.status = 'PENDING'
         ORDER BY dr.label;
        """,
        (contact_id,),
    ).fetchall()
    return [r["label"] for r in rows]


def _contact(db: Connection, contact_id: str) -> dict:
    row = db.execute(
        "SELECT id, first_name, last_name, email, phone, matter_type, dnc, last_sent_at, sends_today "
        "FROM contacts WHERE id = %s;",
        (contact_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "contact not found")
    return dict(row)


def _draft_initial_docs_request(contact_id: str, db: Connection) -> dict:
    # Load contact
    c = _contact(db, contact_id)
    if c.get("dnc"):
        raise HTTPException(400, "contact is DNC")

    # Build/extend portal link via helper (uses env PORTAL_BASE inside build_portal_url)
    portal = build_portal_url(db, contact_id, None)

    # Required & pending labels
    labels = _missing_labels(db, contact_id)

    # Org settings (tone/signature/etc.)
    org = _org_settings(db)

    # ✨ AI-generate the draft (subject/body)
    gen = generate_initial_docs_request(db, c, labels, portal, org)
    subject = gen.get("subject") or "Document Request"
    body = _finalize_body(db, gen.get("body") or "")

    drafted_meta = {
        "intent": gen.get("intent", "initial_docs_request"),
        "subject": subject,
        "confidence": float(gen.get("confidence", 0.92)),
        "labels_included": labels,
        "_llm": gen.get("_llm"),
        "portal_url": portal,
    }

    draft_row = db.execute(
        """
        INSERT INTO messages(contact_id, channel, direction, body, meta)
        VALUES (%s, 'EMAIL', 'DRAFT', %s, %s)
        RETURNING id;
        """,
        (contact_id, body, Json(drafted_meta)),
    ).fetchone()
    draft_id = str(draft_row["id"])

    db.execute(
        "INSERT INTO timeline(contact_id, type, detail) VALUES (%s, 'NOTE', 'Drafted initial docs request (AI)');",
        (contact_id,),
    )
    db.commit()
    return {"ok": True, "draft_id": draft_id, "auto_enqueued": False}


# -----------------------------
# Thread loaders
# -----------------------------

@router.get("/thread/{contact_id}")
def get_thread(contact_id: str, db: Connection = Depends(db_conn)):
    contact = _contact(db, contact_id)
    msgs = db.execute(
        """
        SELECT id, channel, direction, body, meta, created_at
          FROM messages
         WHERE contact_id = %s
         ORDER BY created_at ASC;
        """,
        (contact_id,),
    ).fetchall()
    # psycopg row_factory should be dict_row; if not, convert:
    messages = [dict(r) for r in msgs] if msgs and not isinstance(msgs[0], dict) else msgs
    return {"contact": contact, "messages": messages}


@router.get("/thread/by-message/{message_id}")
def thread_by_message(
    message_id: str,
    db: Connection = Depends(db_conn),
    fallback_contact_id: Optional[str] = Query(default=None),
):
    rec = db.execute(
        """
        SELECT
          m.id              AS mid,
          m.contact_id      AS m_contact_id,
          c.id              AS cid,
          c.first_name, c.last_name, c.email, c.phone, c.matter_type
        FROM messages m
        LEFT JOIN contacts c ON c.id = m.contact_id
        WHERE m.id = %s;
        """,
        (message_id,),
    ).fetchone()
    if not rec:
        raise HTTPException(404, detail=f"message not found: {message_id}")

    if not rec["cid"]:
        if not fallback_contact_id:
            raise HTTPException(
                404,
                detail=f"contact not found for message {message_id} (contact_id={rec['m_contact_id']})",
            )
        c2 = db.execute(
            "SELECT id, first_name, last_name, email, phone, matter_type FROM contacts WHERE id = %s;",
            (fallback_contact_id,),
        ).fetchone()
        if not c2:
            raise HTTPException(404, detail=f"fallback_contact_id does not exist: {fallback_contact_id}")
        db.execute("UPDATE messages SET contact_id = %s WHERE id = %s;", (fallback_contact_id, message_id))
        db.commit()
        contact = dict(c2)
        contact_id = c2["id"]
    else:
        contact = {
            "id": rec["cid"],
            "first_name": rec["first_name"],
            "last_name": rec["last_name"],
            "email": rec["email"],
            "phone": rec["phone"],
            "matter_type": rec["matter_type"],
        }
        contact_id = rec["cid"]

    msgs = db.execute(
        """
        SELECT id, channel, direction, body, meta, created_at
          FROM messages
         WHERE contact_id = %s
         ORDER BY created_at ASC;
        """,
        (contact_id,),
    ).fetchall()
    messages = [dict(r) for r in msgs] if msgs and not isinstance(msgs[0], dict) else msgs
    return {"contact": contact, "messages": messages}


# -----------------------------
# Draft creation (initial outreach)
# -----------------------------

@router.post("/draft-initial-docs/{contact_id}")
def draft_initial_docs_route(contact_id: str, db: Connection = Depends(db_conn)):
    """
    Compose a docs-specific initial draft email (lists required docs + portal link).
    """
    return _draft_initial_docs_request(contact_id, db)


@router.post("/draft-initial/{contact_id}")
def draft_initial(contact_id: str, payload: dict = Body(default={}), db: Connection = Depends(db_conn)):
    """
    Compose initial draft and (optionally) auto-send if org rules allow.
    """
    created = _draft_initial_docs_request(contact_id, db)
    draft_id = created["draft_id"]

    c = db.execute(
        "SELECT dnc, last_sent_at, sends_today FROM contacts WHERE id=%s;",
        (contact_id,),
    ).fetchone()
    if not c:
        raise HTTPException(404, "contact not found")
    org = _org_settings(db)

    allowed, decision_meta, when = should_autosend(
        {
            "org": {
                "require_approval_initial": bool(org.get("require_approval_initial", True)),
                "autosend_confidence_threshold": float(org.get("autosend_confidence_threshold", 0.85)),
                "business_hours_tz": str(org.get("business_hours_tz", "America/Los_Angeles")),
                "business_hours_start": int(org.get("business_hours_start", 8)),
                "business_hours_end": int(org.get("business_hours_end", 18)),
                "cooldown_hours": int(org.get("cooldown_hours", 22)),
                "max_daily_sends": int(org.get("max_daily_sends", 2)),
                "grace_minutes": int(org.get("grace_minutes", 5)),
            },
            "contact": {
                "dnc": bool(c["dnc"]),
                "last_sent_at": c["last_sent_at"],
                "sends_today": c["sends_today"],
            },
            "drafted": {"intent": "initial_docs_request", "confidence": 0.92},
            "is_initial": True,
            "now_utc": datetime.now(timezone.utc),
        }
    )

    db.execute(
        "INSERT INTO timeline(contact_id, type, detail) VALUES (%s,'AUTO_SEND_DECISION',%s);",
        (contact_id, Json({"message_id": draft_id, **(decision_meta or {})})),
    )
    db.commit()

    if not allowed:
        return {"ok": True, "draft_id": draft_id, "auto_enqueued": False}

    # enqueue or schedule
    q = get_queue()
    now_utc = datetime.now(timezone.utc)
    if when and when > now_utc:
        try:
            from rq.scheduler import Scheduler
            scheduler = Scheduler("outbound", connection=q.connection)
            scheduler.enqueue_at(when, "app.jobs.send_message_and_update", draft_id, "EMAIL")
            return {
                "ok": True,
                "draft_id": draft_id,
                "auto_enqueued": True,
                "scheduled_for": when.isoformat(),
            }
        except Exception:
            # fall through to immediate enqueue
            pass

    q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
    return {"ok": True, "draft_id": draft_id, "auto_enqueued": True}


# -----------------------------
# Draft update (edit/save)
# -----------------------------

@router.post("/draft-update/{message_id}")
def update_draft_post(message_id: str, payload: dict = Body(...), db: Connection = Depends(db_conn)):
    new_body = (payload.get("body") or "").strip()
    if not new_body:
        raise HTTPException(422, "body is required")

    row = db.execute(
        """
        UPDATE messages
           SET body = %s
         WHERE id = %s
           AND direction = 'DRAFT'
        RETURNING id, contact_id, channel, direction, body, meta, created_at;
        """,
        (new_body, message_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "draft not found")

    db.commit()
    return {"ok": True, "message": dict(row)}


# -----------------------------
# Approve & send
# -----------------------------

@router.post("/approve/{message_id}")
def approve_and_send(message_id: str, db: Connection = Depends(db_conn)):
    """
    Approve a DRAFT and send.
    Prefer enqueue to RQ; if enqueue fails and INLINE_APPROVE_SEND is on, try inline send.
    """
    row = db.execute(
        "SELECT id, contact_id, channel, direction FROM messages WHERE id=%s;",
        (message_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="message not found")

    if row["direction"] != "DRAFT":
        # Already flipped by worker/previous action
        return {"ok": True, "already_sent": True}

    # timeline note
    db.execute(
        "INSERT INTO timeline(contact_id, type, detail) VALUES (%s,'NOTE','Draft approved by user');",
        (row["contact_id"],),
    )
    db.commit()

    # Try to enqueue first
    try:
        q = get_queue()
        q.enqueue("app.jobs.send_message_and_update", row["id"], row["channel"])
        return {"ok": True, "queued": True}
    except Exception as e_enqueue:
        if not INLINE_APPROVE_SEND:
            raise HTTPException(500, detail=f"Queue failed: {repr(e_enqueue)}")

        # Inline fallback — call same function synchronously
        try:
            from app.jobs import send_message_and_update as _inline_send  # type: ignore
            result = _inline_send(row["id"], row["channel"])
            return {"ok": True, "queued": False, "result": result}
        except Exception as e_inline:
            raise HTTPException(
                status_code=500,
                detail=f"Queue failed: {repr(e_enqueue)} | Inline send failed: {repr(e_inline)}",
            )


# -----------------------------
# (Optional) Scheduler helpers
# -----------------------------

@router.post("/run-nudges-now")
def run_nudges_now():
    q = get_queue()
    job = q.enqueue("app.jobs.nudge_missing_docs")
    return {"enqueued": True, "job_id": job.id}


@router.post("/schedule-nudges")
def schedule_nudges():
    try:
        from rq_scheduler import Scheduler as RQScheduler  # type: ignore
    except Exception:
        raise HTTPException(
            400,
            "rq-scheduler not installed. `pip install rq-scheduler` and run `rqscheduler --url \"$REDIS_URL\"`.",
        )
    q = get_queue()
    scheduler = RQScheduler(queue=q)
    start_utc = datetime.utcnow() + timedelta(seconds=10)
    job = scheduler.schedule(
        scheduled_time=start_utc,
        func="app.jobs.nudge_missing_docs",
        interval=60 * 60 * 24,  # daily
        repeat=None,
    )
    return {"scheduled": True, "first_run_utc": start_utc.isoformat(), "job_id": job.id}

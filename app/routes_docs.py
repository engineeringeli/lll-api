# backend/app/routes_docs.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from typing import Optional
from uuid import UUID
import hashlib
import json
import os
import secrets
import time

from fastapi import APIRouter, Depends, HTTPException, Body, Query
from fastapi.responses import JSONResponse
from psycopg import Connection
from psycopg.types.json import Json

from app.deps import db_conn
from app.queue import get_queue
from app.decisions import should_autosend
from app.followups import generate_initial_docs_request, _portal_url as build_portal_url

router = APIRouter(prefix="/docs", tags=["docs"])

# ---------------------------------------------------------------------
# Config / helpers
# ---------------------------------------------------------------------

BUCKET = (
    os.getenv("UPLOADS_BUCKET")
    or os.getenv("SUPABASE_BUCKET")
    or "uploads"
)

def _json500(detail: str):
    """Uniform JSON 500 so frontends never try to parse HTML."""
    return JSONResponse(status_code=500, content={"detail": detail})

def _json_dumps(obj):
    """Safe JSON dumper for psycopg Json(...)."""
    def _default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, set):
            return list(o)
        return str(o)
    return json.dumps(obj, default=_default)

def _table_exists(db: Connection, name: str) -> bool:
    row = db.execute(
        """
        select exists(
          select 1
            from information_schema.tables
           where table_schema='public'
             and table_name=%s
        ) as ok;
        """,
        (name,),
    ).fetchone()
    return bool(row and row["ok"])

def _contact_exists(db: Connection, contact_id: str) -> bool:
    row = db.execute("select 1 from contacts where id=%s;", (contact_id,)).fetchone()
    return bool(row)

def _get_contact(db: Connection, contact_id: str) -> Optional[dict]:
    row = db.execute(
        """
        select id, first_name, last_name, email, phone, matter_type,
               dnc, last_sent_at, sends_today
          from contacts
         where id=%s;
        """,
        (contact_id,),
    ).fetchone()
    return dict(row) if row else None

def _short_code(label: str) -> str:
    """Short code without pgcrypto, stable enough for display."""
    seed = f"{label}|{time.time_ns()}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

def _ensure_portal_token(db: Connection, contact_id: str, *, ttl_days: int = 30) -> str:
    tok = db.execute(
        """
        select token
          from portal_tokens
         where contact_id=%s
           and (expires_at is null or expires_at > now())
         limit 1;
        """,
        (contact_id,),
    ).fetchone()
    if tok:
        return tok["token"]
    token = secrets.token_urlsafe(24)
    exp = datetime.now(timezone.utc) + timedelta(days=ttl_days)
    db.execute(
        "insert into portal_tokens(token, contact_id, expires_at) values (%s,%s,%s);",
        (token, contact_id, exp),
    )
    return token

def _portal_url(db: Connection, contact_id: str) -> str:
    base = os.getenv("PORTAL_BASE", "http://localhost:3000").rstrip("/")
    token = _ensure_portal_token(db, contact_id, ttl_days=30)
    return f"{base}/portal/{token}"

def _org_settings(db: Connection) -> dict:
    row = db.execute("select * from org_settings limit 1;").fetchone()
    if not row:
        return {
            "require_approval_initial": True,
            "autosend_confidence_threshold": 0.85,
            "business_hours_tz": "America/Los_Angeles",
            "business_hours_start": 8,
            "business_hours_end": 18,
            "cooldown_hours": 22,
            "max_daily_sends": 2,
            "grace_minutes": 5,
            "outbound_from_name": os.getenv("SENDER_NAME", "Law Firm"),
            "include_signature": False,
            "outbound_signature": "",
        }
    d = dict(row)
    d.setdefault("outbound_from_name", os.getenv("SENDER_NAME", "Law Firm"))
    d.setdefault("include_signature", False)
    d.setdefault("outbound_signature", "")
    return d

def _latest_provider_msgid_and_subject(db: Connection, contact_id: str):
    return db.execute(
        """
        select
          meta->>'provider_message_id' as pmid,
          coalesce(meta->>'subject', meta->>'subject_used') as subj
          from messages
         where contact_id = %s
           and (meta->>'provider_message_id') is not null
         order by created_at desc
         limit 1;
        """,
        (contact_id,),
    ).fetchone()

def _all_required_pending_count(db: Connection, contact_id: str) -> int:
    rec = db.execute(
        """
        select count(*) as n
          from client_documents cd
          join document_requirements dr on dr.id = cd.requirement_id
         where cd.contact_id = %s
           and coalesce(cd.is_required, dr.is_required) = true
           and cd.status = 'PENDING';
        """,
        (contact_id,),
    ).fetchone()
    return int(rec["n"] if rec else 0)

# ---------------------------------------------------------------------
# Checklist (lawyer console)
# ---------------------------------------------------------------------

@router.get("/checklist/{contact_id}")
def checklist(contact_id: str, db: Connection = Depends(db_conn)):
    if not _contact_exists(db, contact_id):
        raise HTTPException(404, "contact not found")

    items = db.execute(
        """
        select cd.id as client_doc_id,
               cd.requirement_id,
               dr.code,
               dr.label,
               dr.description,
               dr.is_required,
               cd.status,
               cd.notes,
               cd.uploaded_at,
               cd.reviewed_at,
               cd.source,
               cd.created_by
          from client_documents cd
          join document_requirements dr on dr.id = cd.requirement_id
         where cd.contact_id = %s
         order by dr.label asc;
        """,
        (contact_id,),
    ).fetchall()

    files = db.execute(
        """
        select id, requirement_id, storage_bucket, storage_path, bytes, mime_type, created_at
          from files
         where contact_id = %s
         order by created_at desc;
        """,
        (contact_id,),
    ).fetchall()

    return {"items": items, "files": files}

@router.post("/custom/{contact_id}/add")
def add_custom_requirement(
    contact_id: str,
    payload: dict = Body(...),
    db: Connection = Depends(db_conn),
):
    if not _contact_exists(db, contact_id):
        raise HTTPException(404, "contact not found")

    c = _get_contact(db, contact_id)
    mt = (c.get("matter_type") or "GENERAL").strip()

    label = (payload.get("label") or "").strip()
    description = payload.get("description")
    is_required = bool(payload.get("is_required", True))
    if not label:
        raise HTTPException(400, "label required")

    r = db.execute(
        """
        insert into document_requirements (matter_type, code, label, description, is_required)
        values (%s, %s, %s, %s, %s)
        returning id;
        """,
        (mt, _short_code(label), label, description, is_required),
    ).fetchone()
    req_id = r["id"]

    db.execute(
        """
        insert into client_documents (contact_id, requirement_id, status, source, created_by)
        values (%s, %s, 'PENDING', 'MANUAL', 'LAWYER');
        """,
        (contact_id, req_id),
    )
    db.commit()
    return {"ok": True, "requirement_id": req_id}

@router.post("/custom/{contact_id}/bulk-add")
def bulk_add_requirements(
    contact_id: str,
    payload: dict = Body(...),
    db: Connection = Depends(db_conn),
):
    if not _contact_exists(db, contact_id):
        raise HTTPException(404, "contact not found")

    c = _get_contact(db, contact_id)
    mt = (c.get("matter_type") or "GENERAL").strip()

    labels = payload.get("labels") or []
    labels = [str(x).strip() for x in labels if str(x).strip()]
    if not labels:
        raise HTTPException(400, "labels required")

    added = 0
    for label in labels:
        r = db.execute(
            """
            insert into document_requirements (matter_type, code, label, description, is_required)
            values (%s, %s, %s, null, true)
            returning id;
            """,
            (mt, _short_code(label), label),
        ).fetchone()
        req_id = r["id"]
        db.execute(
            """
            insert into client_documents (contact_id, requirement_id, status, source, created_by)
            values (%s, %s, 'PENDING', 'MANUAL', 'LAWYER');
            """,
            (contact_id, req_id),
        )
        added += 1

    db.commit()
    return {"ok": True, "added": added}

# ---------------------------------------------------------------------
# Kickoff initial outreach for document request (thread-aware)
# ---------------------------------------------------------------------

@router.post("/kickoff/{contact_id}")
def kickoff_docs_request(contact_id: str, db: Connection = Depends(db_conn)):
    c = _get_contact(db, contact_id)
    if not c:
        raise HTTPException(status_code=404, detail="Contact not found")
    if c.get("dnc"):
        raise HTTPException(status_code=400, detail="Contact is DNC")

    # required + pending labels
    missing_rows = db.execute(
        """
        select dr.label
          from client_documents cd
          join document_requirements dr on dr.id = cd.requirement_id
         where cd.contact_id = %s
           and coalesce(cd.is_required, dr.is_required) = true
           and cd.status = 'PENDING'
         order by dr.label;
        """,
        (contact_id,),
    ).fetchall()
    missing_labels = [r["label"] for r in missing_rows]

    # portal + org
    portal = build_portal_url(db, contact_id, os.getenv("PORTAL_BASE", "http://localhost:3000"))
    org = _org_settings(db)

    # draft content
    gen = generate_initial_docs_request(db, c, missing_labels, portal, org)
    subject = gen.get("subject") or "Document Request"
    body = gen.get("body") or ""

    prev = _latest_provider_msgid_and_subject(db, contact_id)
    reply_to = (prev and prev["pmid"]) or None

    meta = {
        "intent": gen.get("intent", "initial_docs_request"),
        "confidence": float(gen.get("confidence", 0.92)),
        "labels": missing_labels,
        "portal": portal,
        "subject": subject,
        "reply_to_message_id": reply_to,
        "_llm": gen.get("_llm"),
    }

    draft_row = db.execute(
        """
        insert into messages(contact_id, channel, direction, body, meta)
        values (%s, 'EMAIL', 'DRAFT', %s, %s)
        returning id;
        """,
        (contact_id, body, Json(meta, dumps=_json_dumps)),
    ).fetchone()
    draft_id = str(draft_row["id"])

    db.execute(
        "insert into timeline(contact_id, type, detail) values (%s,'NOTE','Initial docs request drafted (AI)');",
        (contact_id,),
    )

    # auto-send decision (initial)
    now_utc = datetime.now(timezone.utc)
    allowed, decision_meta, when = should_autosend(
        {
            "org": {
                "require_approval_initial": bool(org["require_approval_initial"]),
                "autosend_confidence_threshold": float(org["autosend_confidence_threshold"]),
                "business_hours_tz": org["business_hours_tz"],
                "business_hours_start": int(org["business_hours_start"]),
                "business_hours_end": int(org["business_hours_end"]),
                "cooldown_hours": int(org["cooldown_hours"]),
                "max_daily_sends": int(org["max_daily_sends"]),
                "grace_minutes": int(org["grace_minutes"]),
            },
            "contact": {
                "dnc": bool(c["dnc"]),
                "last_sent_at": c["last_sent_at"],
                "sends_today": c["sends_today"],
            },
            "drafted": meta,
            "is_initial": True,
            "now_utc": now_utc,
        }
    )

    db.execute(
        "insert into timeline(contact_id, type, detail) values (%s,'AUTO_SEND_DECISION',%s);",
        (contact_id, Json({"message_id": draft_id, **(decision_meta or {})}, dumps=_json_dumps)),
    )
    db.commit()

    # enqueue if allowed (best-effort)
    auto_enqueued = False
    scheduled_for = None
    if allowed:
        q = get_queue()
        try:
            if when and when > now_utc:
                from rq.scheduler import Scheduler
                scheduler = Scheduler("outbound", connection=q.connection)
                scheduler.enqueue_at(when, "app.jobs.send_message_and_update", draft_id, "EMAIL")
                auto_enqueued = True
                scheduled_for = when.isoformat()
            else:
                q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
                auto_enqueued = True
        except Exception:
            # non-fatal; leave as draft
            pass

    return {
        "id": draft_id,
        "auto_enqueued": auto_enqueued,
        "scheduled_for": scheduled_for,
        "portal": portal,
        "labels": missing_labels,
        "subject": subject,
    }

# ---------------------------------------------------------------------
# Review actions (lawyer console)
# ---------------------------------------------------------------------

@router.post("/review/approve")
def approve_doc(payload: dict = Body(...), db: Connection = Depends(db_conn)):
    contact_id = (payload.get("contact_id") or "").strip()
    requirement_id = (payload.get("requirement_id") or "").strip()
    if not contact_id or not requirement_id:
        raise HTTPException(400, "contact_id and requirement_id required")

    row = db.execute(
        """
        select id, status
          from client_documents
         where contact_id = %s and requirement_id = %s;
        """,
        (contact_id, requirement_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "client_documents row not found")

    db.execute(
        """
        update client_documents
           set status = 'APPROVED',
               reviewed_at = now(),
               notes = notes
         where contact_id = %s
           and requirement_id = %s;
        """,
        (contact_id, requirement_id),
    )

    # If checklist complete (no required pending), enqueue courtesy note
    if _all_required_pending_count(db, contact_id) == 0:
        try:
            q = get_queue()
            q.enqueue("app.jobs.on_all_docs_received", contact_id)
        except Exception:
            pass

    db.commit()
    return {"ok": True, "status": "APPROVED"}

@router.post("/review/reject")
def reject_upload(
    payload: dict = Body(...),
    db: Connection = Depends(db_conn),
):
    contact_id = (payload.get("contact_id") or "").strip()
    requirement_id = (payload.get("requirement_id") or "").strip()
    reason = (payload.get("reason") or "").strip()
    create_followup = bool(payload.get("create_followup_draft", True))

    if not contact_id or not requirement_id:
        raise HTTPException(400, "contact_id and requirement_id required")
    if not _contact_exists(db, contact_id):
        raise HTTPException(404, "contact not found")

    row = db.execute(
        """
        update client_documents
           set status = 'REJECTED',
               notes = %s,
               reviewed_at = now()
         where contact_id = %s and requirement_id = %s
        returning id;
        """,
        (reason or None, contact_id, requirement_id),
    ).fetchone()
    if not row:
        raise HTTPException(404, "client document not found")

    followup = None
    if create_followup:
        portal = _portal_url(db, contact_id)
        body = (
            "Thanks for the upload. It looks like we need a clearer copy or a different document.\n\n"
            f"Reason: {reason or 'See notes'}\n\n"
            f"Please use your upload link to resubmit: {portal}\n\n"
            "Thank you!"
        )
        msg = db.execute(
            """
            insert into messages (contact_id, channel, direction, body, meta)
            values (%s, 'EMAIL', 'DRAFT', %s, %s)
            returning id;
            """,
            (contact_id, body, Json({"intent": "doc_fix"}, dumps=_json_dumps)),
        ).fetchone()
        followup = {"draft_id": str(msg["id"])}

    db.execute(
        "insert into timeline (contact_id, type, detail) values (%s,'NOTE','Document rejected');",
        (contact_id,),
    )
    db.commit()
    return {"ok": True, "followup": followup}

# ---------------------------------------------------------------------
# Client portal: magic link + portal payload + upload record
# ---------------------------------------------------------------------

@router.post("/magic-link/{contact_id}")
def create_magic_link(contact_id: str, db: Connection = Depends(db_conn)):
    exists = db.execute("select 1 from contacts where id=%s;", (contact_id,)).fetchone()
    if not exists:
        raise HTTPException(404, "contact not found")

    expires_at = datetime.now(timezone.utc) + timedelta(days=7)

    has_portal = _table_exists(db, "portal_tokens")
    has_magic  = _table_exists(db, "magic_links")

    if has_portal:
        try:
            token = secrets.token_urlsafe(24)
            db.execute(
                "insert into portal_tokens(token, contact_id, expires_at) values (%s,%s,%s);",
                (token, contact_id, expires_at),
            )
            db.commit()
            return {"token": token, "expires_at": expires_at.isoformat()}
        except Exception as e1:
            try: db.rollback()
            except Exception: pass
            if not has_magic:
                return _json500(f"portal_tokens insert failed and magic_links not present: {e1}")
            try:
                rec = db.execute(
                    """
                    insert into magic_links (contact_id, purpose, expires_at)
                    values (%s, 'UPLOAD', %s)
                    returning token;
                    """,
                    (contact_id, expires_at),
                ).fetchone()
                db.commit()
                return {"token": str(rec["token"]), "expires_at": expires_at.isoformat()}
            except Exception as e2:
                try: db.rollback()
                except Exception: pass
                return _json500(f"magic_links fallback failed: {e2}")

    elif has_magic:
        try:
            rec = db.execute(
                """
                insert into magic_links (contact_id, purpose, expires_at)
                values (%s, 'UPLOAD', %s)
                returning token;
                """,
                (contact_id, expires_at),
            ).fetchone()
            db.commit()
            return {"token": str(rec["token"]), "expires_at": expires_at.isoformat()}
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            return _json500(f"magic_links insert failed: {e}")

    else:
        return _json500("neither portal_tokens nor magic_links table exists; create one")

@router.get("/portal/{token}")
def portal_init(token: str, db: Connection = Depends(db_conn)):
    try:
        rec = db.execute(
            """
            with t as (
              select contact_id, expires_at from portal_tokens where token = %s
              union all
              select contact_id, expires_at from magic_links where token::text = %s
            )
            select t.contact_id, t.expires_at,
                   c.first_name, c.last_name, c.email, c.phone, c.matter_type
              from t
              join contacts c on c.id = t.contact_id
             limit 1;
            """,
            (token, token),
        ).fetchone()

        if not rec:
            raise HTTPException(404, "invalid or unknown link")
        if rec["expires_at"] and rec["expires_at"] < datetime.now(timezone.utc):
            raise HTTPException(410, "link expired")

        items = db.execute(
            """
            select cd.id as client_doc_id,
                   cd.requirement_id,
                   dr.code,
                   dr.label,
                   dr.description,
                   dr.is_required,
                   cd.status,
                   cd.notes,
                   cd.uploaded_at,
                   cd.reviewed_at
              from client_documents cd
              join document_requirements dr on dr.id = cd.requirement_id
             where cd.contact_id = %s
             order by dr.label asc;
            """,
            (rec["contact_id"],),
        ).fetchall()

        return {
            "contact": {
                "id": rec["contact_id"],
                "first_name": rec["first_name"],
                "last_name": rec["last_name"],
                "email": rec["email"],
                "phone": rec["phone"],
                "matter_type": rec["matter_type"],
            },
            "items": items,
            "expires_at": rec["expires_at"].isoformat() if rec["expires_at"] else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        return _json500(f"internal error: {e}")

@router.post("/portal/{token}/upload")
def portal_upload(
    token: str,
    payload: dict = Body(...),
    db: Connection = Depends(db_conn),
):
    """
    Record the upload, flip status to UPLOADED, and enqueue a polite follow-up.
    """
    try:
        t = db.execute(
            "select contact_id, expires_at from portal_tokens where token=%s;",
            (token,),
        ).fetchone()
        if not t:
            raise HTTPException(404, "invalid or unknown link")
        if t["expires_at"] and t["expires_at"] < datetime.now(timezone.utc):
            raise HTTPException(410, "link expired")

        contact_id = t["contact_id"]
        requirement_id = payload.get("requirement_id")
        storage_path = payload.get("storage_path")
        bytes_ = payload.get("bytes")
        mime_type = payload.get("mime_type")

        if not requirement_id or not storage_path:
            raise HTTPException(400, "requirement_id and storage_path required")

        exists = db.execute(
            "select 1 from client_documents where contact_id=%s and requirement_id=%s;",
            (contact_id, requirement_id),
        ).fetchone()
        if not exists:
            raise HTTPException(404, "document requirement for this contact not found; add it first")

        db.execute(
            """
            insert into files(contact_id, requirement_id, storage_bucket, storage_path, bytes, mime_type)
            values (%s, %s, %s, %s, %s, %s);
            """,
            (contact_id, requirement_id, BUCKET, storage_path, bytes_, mime_type),
        )
        db.execute(
            """
            update client_documents
               set status = 'UPLOADED', uploaded_at = now()
             where contact_id = %s and requirement_id = %s;
            """,
            (contact_id, requirement_id),
        )
        db.commit()

        try:
            q = get_queue()
            q.enqueue("app.jobs.on_client_upload", contact_id, requirement_id)
        except Exception:
            pass

        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        return _json500(f"internal error: {str(e)}")

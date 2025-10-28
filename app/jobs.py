# backend/app/jobs.py
from __future__ import annotations

from app.followups import (
    classify_inbound,
    draft_followup_for_missing,
    draft_ack_for_inbound,   # accepts missing_labels
)
from app.decisions import should_autosend
from app.followups import _portal_url as build_portal_url

import os, json, time, requests, secrets
from uuid import uuid4, UUID
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone, date
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from dotenv import load_dotenv
load_dotenv(override=True)

# --- Queue plumbing (single source of truth) ---
from app.queue import get_queue, QUEUE_NAME

# ============================================================
# ENV / CONFIG
# ============================================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing in backend/.env")

# If true, skip actual provider sends (helpful for demos)
DEMO_SEND = os.getenv("DEMO_SEND", "false").lower() in ("1", "true", "yes", "on")

# --- Email (SendGrid) ---
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")         # REQUIRED in prod
SENDER_EMAIL     = os.getenv("SENDER_EMAIL")             # REQUIRED in prod (e.g. no-reply@yourdomain.com)
SENDER_NAME      = os.getenv("SENDER_NAME", "Law Firm")

# Inbound-reply addressing for SendGrid Inbound Parse
# Reply-To will be: {REPLIES_PREFIX}+{contact_id}@{REPLIES_DOMAIN}
REPLIES_DOMAIN = os.getenv("REPLIES_DOMAIN")             # e.g. inbound.yourdomain.com
REPLIES_PREFIX = os.getenv("REPLIES_PREFIX", "r")

# --- SMS (Twilio) ---
from twilio.rest import Client
TWILIO_SID   = os.getenv("TWILIO_ACCOUNT_SID")           # REQUIRED in prod if sending SMS
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM  = os.getenv("TWILIO_FROM_NUMBER")           # +1XXXXXXXXXX

# --- Portal base (for magic links in follow-ups) ---
PORTAL_BASE = os.getenv("PORTAL_BASE", "http://localhost:3000")

# Follow-up heuristics
FOLLOWUP_DAYS = int(os.getenv("FOLLOWUP_DAYS", "2"))

# ============================================================
# JSON helper (safe for UUID, datetime, set, etc.)
# ============================================================
def _json_dumps(obj):
    def _default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, set):
            return list(o)
        return str(o)
    return json.dumps(obj, default=_default)

# ============================================================
# DB helper
# ============================================================
@contextmanager
def _db():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False) as conn:
        yield conn

def _get_queue():
    # kept for minimal call-site changes; uses shared QUEUE_NAME internally
    return get_queue()

# ============================================================
# Org settings helper
# ============================================================
def _org_settings(conn):
    row = conn.execute("SELECT * FROM org_settings LIMIT 1;").fetchone()
    if not row:
        # sane defaults if table empty
        return {
            "require_approval_initial": True,
            "autosend_confidence_threshold": 0.85,
            "business_hours_tz": "America/Los_Angeles",
            "business_hours_start": 8,
            "business_hours_end": 18,
            "cooldown_hours": 22,
            "max_daily_sends": 2,
            "grace_minutes": 5,
        }
    return dict(row)

# ============================================================
# Threading helpers
# ============================================================
def _mail_domain() -> str:
    if REPLIES_DOMAIN:
        return REPLIES_DOMAIN.split("@")[-1]
    if SENDER_EMAIL and "@" in SENDER_EMAIL:
        return SENDER_EMAIL.split("@")[-1]
    return "mailer.local"

def _make_message_id() -> str:
    # RFC 5322-ish angle-bracketed ID
    return f"<{uuid4().hex}.{int(time.time())}@{_mail_domain()}>"

def _latest_provider_msgid_and_subject(conn, contact_id: str):
    """
    Returns the most recent provider_message_id and subject used for a contact, if any.
    """
    return conn.execute(
        """
        SELECT
          meta->>'provider_message_id' AS pmid,
          COALESCE(meta->>'subject', meta->>'subject_used') AS subj
        FROM messages
        WHERE contact_id = %s
          AND (meta->>'provider_message_id') IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (contact_id,),
    ).fetchone()

# ============================================================
# Providers
# ============================================================
def send_email(
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    *,
    contact_id: str,
    message_id: str,
    from_name: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,  # threading headers etc.
) -> Dict[str, Any]:
    """
    Send via SendGrid HTTP API. Adds:
      - Reply-To: r+{contact_id}@{REPLIES_DOMAIN}
      - X-Contact-ID / X-Message-ID headers
      - Optional threading headers (Message-ID, In-Reply-To, References)
    """
    assert SENDGRID_API_KEY, "Missing SENDGRID_API_KEY in backend/.env"
    assert SENDER_EMAIL,     "Missing SENDER_EMAIL in backend/.env"
    assert REPLIES_DOMAIN,   "Set REPLIES_DOMAIN=inbound.yourdomain.com in backend/.env"

    contact_id = str(contact_id)
    message_id = str(message_id)

    reply_to_email = f"{REPLIES_PREFIX}+{contact_id}@{REPLIES_DOMAIN}"

    url = "https://api.sendgrid.com/v3/mail/send"
    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type": "application/json",
    }
    content = [{"type": "text/plain", "value": body_text}]
    if body_html:
        content.append({"type": "text/html", "value": body_html})

    personalization_headers = {
        "X-Contact-ID": contact_id,
        "X-Message-ID": message_id,
    }
    if extra_headers:
        personalization_headers.update(extra_headers)

    data = {
        "personalizations": [{
            "to": [{"email": to_email}],
            "headers": personalization_headers,
        }],
        "from": {"email": SENDER_EMAIL, "name": (from_name or SENDER_NAME)},
        "reply_to": {"email": reply_to_email},
        "subject": subject,
        "content": content,
    }

    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=20)
    return {
        "status": r.status_code,
        "text": r.text,
        "reply_to": reply_to_email,
        "message_id": personalization_headers.get("Message-ID"),  # echo back what we set
    }

def send_sms(to_number: str, body_text: str) -> Dict[str, Any]:
    """
    Send via Twilio. Returns {sid, status}.
    """
    assert TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM, "Missing Twilio env (SID/TOKEN/FROM) in backend/.env"
    client = Client(TWILIO_SID, TWILIO_TOKEN)
    msg = client.messages.create(
        to=to_number,
        from_=TWILIO_FROM,
        body=body_text,
    )
    for _ in range(3):
        msg = client.messages(msg.sid).fetch()
        if msg.status in ("queued", "sending", "sent"):
            time.sleep(1)
        else:
            break
    return {"sid": msg.sid, "status": msg.status}

# ============================================================
# Finalize send: flip DRAFT -> OUTBOUND & log (+ persist threading info)
# ============================================================
def _finalize_send(
    conn,
    message_id: str,
    provider_meta: dict,
    *,
    subject_used: Optional[str] = None,
    provider_message_id: Optional[str] = None,
):
    row = conn.execute("SELECT contact_id FROM messages WHERE id = %s;", (message_id,)).fetchone()
    if not row:
        return
    contact_id = row["contact_id"]

    meta_patch = {"provider_result": provider_meta}
    if subject_used:
        meta_patch["subject"] = subject_used
    if provider_message_id:
        meta_patch["provider_message_id"] = provider_message_id

    conn.execute(
        """
        UPDATE messages
           SET direction='OUTBOUND',
               meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
         WHERE id = %s;
        """,
        (Json(meta_patch, dumps=_json_dumps), message_id),
    )
    conn.execute(
        "UPDATE contacts SET sends_today = sends_today + 1, last_sent_at = NOW(), updated_at = NOW() WHERE id = %s;",
        (contact_id,),
    )
    conn.execute(
        "INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'NOTE','Message sent via worker (threaded)');",
        (contact_id,),
    )
    conn.commit()

# ============================================================
# RQ Job: send message and update DB (thread-aware)
# ============================================================
def send_message_and_update(message_id: str, channel: str) -> dict:
    """
    1) Load message + destination + org from_name
    2) Compute subject + threading headers
    3) Send via provider (or demo skip)
    4) Finalize DB state in a fresh connection (including provider_message_id + subject)
    """
    # 1) Load everything we need while the connection is open
    with _db() as conn:
        row = conn.execute(
            """
            SELECT
                m.id         AS message_id,
                m.contact_id AS contact_id,
                m.body       AS body,
                m.meta       AS meta,
                c.email      AS email,
                c.phone      AS phone,
                (SELECT outbound_from_name FROM org_settings LIMIT 1) AS outbound_from_name
            FROM messages m
            JOIN contacts c ON c.id = m.contact_id
            WHERE m.id = %s;
            """,
            (message_id,),
        ).fetchone()

        if not row:
            return {"error": "message not found"}

        meta = row.get("meta") or {}
        if not isinstance(meta, dict):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        # Determine base subject
        base_subject = (meta.get("subject") or meta.get("thread_subject") or "").strip()
        if not base_subject:
            prev_subj = conn.execute(
                """
                SELECT meta->>'subject' AS subj
                FROM messages
                WHERE contact_id = %s AND meta ? 'subject'
                ORDER BY created_at ASC
                LIMIT 1;
                """,
                (row["contact_id"],),
            ).fetchone()
            base_subject = (prev_subj and prev_subj["subj"]) or "Regarding your case"

        # Prefix Re: for follow-ups unless already present (keep initial as-is)
        subject = base_subject
        if meta.get("intent") not in ("initial_docs_request",) and not base_subject.lower().startswith("re:"):
            subject = f"Re: {base_subject}"

        # Threading parent
        parent_pmid = meta.get("reply_to_message_id")
        if not parent_pmid:
            prev = _latest_provider_msgid_and_subject(conn, row["contact_id"])
            if prev and prev["pmid"]:
                parent_pmid = prev["pmid"]

    # 2) Build threading headers
    new_msgid = _make_message_id()
    extra_headers: Dict[str, str] = {"Message-ID": new_msgid}
    if parent_pmid:
        if not str(parent_pmid).startswith("<"):
            parent_pmid = f"<{parent_pmid}>"
        extra_headers["In-Reply-To"] = parent_pmid
        extra_headers["References"]  = parent_pmid

    # 3) Send (or demo)
    if DEMO_SEND:
        result = {"demo": True, "note": "Skipped real provider send", "message_id": new_msgid}
    else:
        if channel == "SMS":
            if not row["phone"]:
                return {"error": "contact has no phone"}
            result = send_sms(row["phone"], row["body"])
        else:
            if not row["email"]:
                return {"error": "contact has no email"}
            result = send_email(
                to_email=row["email"],
                subject=subject or "Quick check re your documents",
                body_text=row["body"],
                contact_id=row["contact_id"],
                message_id=row["message_id"],
                from_name=row.get("outbound_from_name") or SENDER_NAME,
                extra_headers=extra_headers,
            )

    # 4) Finalize in a NEW connection (persist subject + provider_message_id)
    provider_msgid = result.get("message_id") or new_msgid
    with _db() as conn2:
        _finalize_send(
            conn2,
            message_id,
            result,
            subject_used=subject,
            provider_message_id=provider_msgid,
        )

    return {"ok": True, "result": result, "provider_message_id": provider_msgid, "subject": subject}

# ============================================================
# on_client_upload: draft ack + remaining needs (thread-aware)
# ============================================================
def on_client_upload(contact_id: str, requirement_id: str | None = None) -> dict:
    """
    Event-driven nudge after a client uploads a document.

    - Thanks them for the received item (if we know which one)
    - Lists remaining required items still in PENDING
    - Includes the portal link
    - Auto-sends if org rules allow; otherwise leaves as DRAFT
    """
    with _db() as db:
        # sanity
        c = db.execute("SELECT * FROM contacts WHERE id=%s;", (contact_id,)).fetchone()
        if not c or c.get("dnc"):
            return {"ok": False, "reason": "missing contact or DNC"}

        # Which label did we just receive (optional)
        received_label = None
        if requirement_id:
            r = db.execute(
                "SELECT label FROM document_requirements WHERE id=%s;",
                (requirement_id,)
            ).fetchone()
            if r:
                received_label = r["label"]

        # What is still missing (required + pending)
        missing = db.execute(
            """
            SELECT dr.label
              FROM client_documents cd
              JOIN document_requirements dr ON dr.id = cd.requirement_id
             WHERE cd.contact_id = %s
               AND COALESCE(cd.is_required, dr.is_required) = TRUE
               AND cd.status = 'PENDING'
             ORDER BY dr.label;
            """,
            (contact_id,)
        ).fetchall()
        missing_labels = [m["label"] for m in missing]

        # Ensure a valid portal token (30d) for the CTA
        tok = db.execute(
            "SELECT token FROM portal_tokens WHERE contact_id=%s AND (expires_at IS NULL OR expires_at>now()) LIMIT 1",
            (contact_id,)
        ).fetchone()
        if not tok:
            token = secrets.token_urlsafe(24)
            exp = datetime.now(timezone.utc) + timedelta(days=30)
            db.execute(
                "INSERT INTO portal_tokens(token, contact_id, expires_at) VALUES (%s,%s,%s)",
                (token, contact_id, exp)
            )
            portal_token = token
        else:
            portal_token = tok["token"]

        portal_url = f"{PORTAL_BASE}/portal/{portal_token}"

        # Compose message
        first = (c.get("first_name") or "").strip()
        name_part = f" {first}" if first else ""

        if missing_labels:
            need_list = ", ".join(missing_labels[:3]) + (f" and {len(missing_labels)-3} more" if len(missing_labels) > 3 else "")
            intro = f"Thanks{name_part}! We received your **{received_label}**." if received_label else f"Thanks{name_part}! We received your upload."
            body = (
                f"{intro}\n\n"
                f"To keep things moving, we still need: {need_list}.\n"
                f"You can securely upload the rest here: {portal_url}\n\n"
                f"If anything is tricky, reply here and we’ll help."
            )
        else:
            intro = f"Thanks{name_part}! We received your **{received_label}**." if received_label else f"Thanks{name_part}! We received your upload."
            body = (
                f"{intro}\n\n"
                f"That completes your checklist — you’re all set for now. "
                f"If you have any questions, just reply here."
            )

        # Thread under last provider message if possible
        prev = _latest_provider_msgid_and_subject(db, contact_id)
        reply_to = (prev and prev["pmid"]) or None
        subject = (prev and prev["subj"]) or "Documents for your case"
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"

        # Save as DRAFT
        drafted_meta = {
            "intent": "upload_ack",
            "confidence": 0.92,
            "source": "on_client_upload",
            "reply_to_message_id": reply_to,
            "subject": subject,
        }
        draft_row = db.execute(
            "INSERT INTO messages(contact_id, channel, direction, body, meta) VALUES (%s,'EMAIL','DRAFT',%s,%s) RETURNING id;",
            (contact_id, body, Json(drafted_meta, dumps=_json_dumps))
        ).fetchone()
        draft_id = str(draft_row["id"])

        db.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','Drafted upload ack with remaining needs');",
            (contact_id,)
        )

        # Decide auto-send (FOLLOW-UP rules)
        org = _org_settings(db)
        now_utc = datetime.now(timezone.utc)
        allowed, decision_meta, when = should_autosend(
            {
                "org": {
                    "require_approval_initial": org["require_approval_initial"],
                    "autosend_confidence_threshold": float(org["autosend_confidence_threshold"]),
                    "business_hours_tz": org["business_hours_tz"],
                    "business_hours_start": org["business_hours_start"],
                    "business_hours_end": org["business_hours_end"],
                    "cooldown_hours": org["cooldown_hours"],
                    "max_daily_sends": org["max_daily_sends"],
                    "grace_minutes": org["grace_minutes"],
                },
                "contact": {
                    "dnc": c["dnc"],
                    "last_sent_at": c["last_sent_at"],
                    "sends_today": c["sends_today"],
                },
                "drafted": drafted_meta,
                "is_initial": False,   # this is a follow-up
                "now_utc": now_utc,
            }
        )

        db.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'AUTO_SEND_DECISION',%s);",
            (contact_id, Json({"message_id": draft_id, **(decision_meta or {})}, dumps=_json_dumps))
        )
        db.commit()

        if not allowed:
            return {"ok": True, "draft_id": draft_id, "auto_enqueued": False}

        # Enqueue send
        q = _get_queue()
        if when and when > now_utc:
            try:
                from rq.scheduler import Scheduler
                scheduler = Scheduler(QUEUE_NAME, connection=q.connection)
                print(f"[scheduler] enqueue_at queue={QUEUE_NAME} message_id={draft_id} when={when.isoformat()}")
                scheduler.enqueue_at(when, "app.jobs.send_message_and_update", draft_id, "EMAIL")
                return {"ok": True, "draft_id": draft_id, "auto_enqueued": True, "scheduled_for": when.isoformat()}
            except Exception:
                pass

        print(f"[queue] enqueue send_message_and_update id={draft_id} q={QUEUE_NAME}")
        q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
        return {"ok": True, "draft_id": draft_id, "auto_enqueued": True}

# ============================================================
# Follow-up engine hooks
# ============================================================
def nudge_missing_docs():
    """
    Periodic job: for contacts with required docs still pending AND
    no inbound in the last FOLLOWUP_DAYS, draft a follow-up.
    """
    since = datetime.now(timezone.utc) - timedelta(days=FOLLOWUP_DAYS)

    with _db() as db:
        rows = db.execute(
            """
            WITH needed AS (
              SELECT cd.contact_id, array_agg(dr.label ORDER BY dr.label) AS missing_labels
                FROM client_documents cd
                JOIN document_requirements dr ON dr.id = cd.requirement_id
               WHERE cd.status = 'PENDING'
                 AND COALESCE(cd.is_required, dr.is_required) = TRUE
               GROUP BY cd.contact_id
            ),
            recent_inbound AS (
              SELECT contact_id, MAX(created_at) AS last_in
                FROM messages
               WHERE direction = 'INBOUND'
               GROUP BY contact_id
            )
            SELECT n.contact_id, n.missing_labels, COALESCE(ri.last_in, '1970-01-01'::timestamptz) AS last_in
              FROM needed n
              LEFT JOIN recent_inbound ri ON ri.contact_id = n.contact_id
             WHERE array_length(n.missing_labels,1) > 0
               AND COALESCE(ri.last_in, '1970-01-01'::timestamptz) < %s;
            """,
            (since,),
        ).fetchall()

        for r in rows:
            cid = r["contact_id"]
            # Ensure a valid portal token (30 days)
            tok = db.execute(
                "SELECT token FROM portal_tokens WHERE contact_id=%s AND (expires_at IS NULL OR expires_at>now()) LIMIT 1",
                (cid,),
            ).fetchone()
            if not tok:
                token = secrets.token_urlsafe(24)
                exp = datetime.now(timezone.utc) + timedelta(days=30)
                db.execute(
                    "INSERT INTO portal_tokens(token, contact_id, expires_at) VALUES (%s,%s,%s)",
                    (token, cid, exp),
                )
                portal_url = f"{PORTAL_BASE}/portal/{token}"
            else:
                portal_url = f"{PORTAL_BASE}/portal/{tok['token']}"

            mid = draft_followup_for_missing(db, cid, r["missing_labels"], portal_url)
            print(f"[nudge_missing_docs] drafted follow-up {mid} for contact {cid}")
        db.commit()

# ============================================================
# Optional helpers to queue doc followups in bulk
# ============================================================
def enqueue_doc_followups():
    with _db() as conn:
        rows = conn.execute(
            """
            SELECT c.id AS contact_id
              FROM contacts c
             WHERE EXISTS (
                SELECT 1
                  FROM client_documents cd
                 WHERE cd.contact_id=c.id
                   AND cd.status='PENDING'
             );
            """
        ).fetchall()
        q = _get_queue()
        for r in rows:
            q.enqueue("app.jobs.make_doc_followup_draft", r["contact_id"])

def make_doc_followup_draft(contact_id: str):
    with _db() as conn:
        pending = conn.execute(
            """
            SELECT dr.label
              FROM client_documents cd
              JOIN document_requirements dr ON dr.id=cd.requirement_id
             WHERE cd.contact_id=%s AND cd.status='PENDING';
            """,
            (contact_id,),
        ).fetchall()
        labels = ", ".join([p["label"] for p in pending]) or "documents"

        # ensure a short-lived (e.g. 2h) magic link via portal_tokens
        tok = conn.execute(
            "SELECT token FROM portal_tokens WHERE contact_id=%s AND (expires_at IS NULL OR expires_at>now()) LIMIT 1",
            (contact_id,),
        ).fetchone()
        if not tok:
            exp = datetime.now(timezone.utc) + timedelta(hours=2)
            token = secrets.token_urlsafe(24)
            conn.execute(
                "INSERT INTO portal_tokens(token, contact_id, expires_at) VALUES (%s,%s,%s)",
                (token, contact_id, exp),
            )
            portal_url = f"{PORTAL_BASE}/portal/{token}"
        else:
            portal_url = f"{PORTAL_BASE}/portal/{tok['token']}"

        # Thread under last message
        prev = _latest_provider_msgid_and_subject(conn, contact_id)
        reply_to = (prev and prev["pmid"]) or None
        subject = (prev and prev["subj"]) or "Documents for your case"
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"

        body = (
            f"Quick reminder — we still need: {labels}.\n\n"
            f"You can securely upload here: {portal_url}\n\n"
            f"Thank you!"
        )

        mid = conn.execute(
            """
            INSERT INTO messages(contact_id, channel, direction, body, meta)
            VALUES (%s,'EMAIL','DRAFT',%s,%s)
            RETURNING id;
            """,
            (contact_id, body, Json({"intent": "doc_followup", "reply_to_message_id": reply_to, "subject": subject}, dumps=_json_dumps)),
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','Doc follow-up draft created');",
            (contact_id,),
        )
        conn.commit()
        return {"draft_id": mid}

# ============================================================
# All docs received → courteous confirmation (call this when nothing pending)
# ============================================================
def on_all_docs_received(contact_id: str) -> dict:
    with _db() as db:
        c = db.execute("SELECT * FROM contacts WHERE id=%s;", (contact_id,)).fetchone()
        if not c or c.get("dnc"):
            return {"ok": False, "reason": "missing contact or DNC"}

        # Confirm there are truly no required PENDING items
        r = db.execute(
            """
            SELECT COUNT(*) AS n
            FROM client_documents cd
            JOIN document_requirements dr ON dr.id = cd.requirement_id
            WHERE cd.contact_id=%s
              AND COALESCE(cd.is_required, dr.is_required) = TRUE
              AND cd.status = 'PENDING';
            """,
            (contact_id,),
        ).fetchone()
        if r["n"] != 0:
            return {"ok": False, "reason": "still pending"}

        # Thread to last message
        prev = _latest_provider_msgid_and_subject(db, contact_id)
        reply_to = (prev and prev["pmid"]) or None
        thread_subject = (prev and prev["subj"]) or "Documents for your case"
        subject = thread_subject if thread_subject.lower().startswith("re:") else f"Re: {thread_subject}"

        first = (c.get("first_name") or "").strip()
        hi = f"Hi {first}," if first else "Hi,"

        body = (
            f"{hi}\n\n"
            "Great news — we’ve received everything we need from you for now. "
            "Our team will review and be in touch about next steps.\n\n"
            "If anything else comes up, we’ll let you know. Thanks again!"
        )

        meta = {
            "intent": "all_docs_received",
            "reply_to_message_id": reply_to,
            "subject": subject,
        }

        draft = db.execute(
            "INSERT INTO messages(contact_id,channel,direction,body,meta) VALUES (%s,'EMAIL','DRAFT',%s,%s) RETURNING id;",
            (contact_id, body, Json(meta, dumps=_json_dumps)),
        ).fetchone()
        draft_id = draft["id"]
        db.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','All docs received draft created');",
            (contact_id,),
        )

        # Follow-up autosend rules
        org = _org_settings(db)
        now_utc = datetime.now(timezone.utc)
        allowed, decision_meta, when = should_autosend(
            {
                "org": {
                    "require_approval_initial": org["require_approval_initial"],
                    "autosend_confidence_threshold": float(org["autosend_confidence_threshold"]),
                    "business_hours_tz": org["business_hours_tz"],
                    "business_hours_start": org["business_hours_start"],
                    "business_hours_end": org["business_hours_end"],
                    "cooldown_hours": org["cooldown_hours"],
                    "max_daily_sends": org["max_daily_sends"],
                    "grace_minutes": org["grace_minutes"],
                },
                "contact": {
                    "dnc": c["dnc"],
                    "last_sent_at": c["last_sent_at"],
                    "sends_today": c["sends_today"],
                },
                "drafted": {"intent": "all_docs_received", "confidence": 1.0},
                "is_initial": False,
                "now_utc": now_utc,
            }
        )
        db.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'AUTO_SEND_DECISION',%s);",
            (contact_id, Json({"message_id": draft_id, **(decision_meta or {})}, dumps=_json_dumps)),
        )
        db.commit()

        if not allowed:
            return {"ok": True, "draft_id": draft_id, "auto_enqueued": False}

        q = _get_queue()
        if when and when > now_utc:
            try:
                from rq.scheduler import Scheduler
                scheduler = Scheduler(QUEUE_NAME, connection=q.connection)
                print(f"[scheduler] enqueue_at queue={QUEUE_NAME} message_id={draft_id} when={when.isoformat()}")
                scheduler.enqueue_at(when, "app.jobs.send_message_and_update", draft_id, "EMAIL")
                return {"ok": True, "draft_id": draft_id, "auto_enqueued": True, "scheduled_for": when.isoformat()}
            except Exception:
                pass

        print(f"[queue] enqueue send_message_and_update id={draft_id} q={QUEUE_NAME}")
        q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
        return {"ok": True, "draft_id": draft_id, "auto_enqueued": True}

# ============================================================
# React to inbound (classify + LLM reply, thread + mention missing)
# ============================================================
def react_to_inbound(message_id: str):
    """
    When an INBOUND email arrives:
      - classify for DNC / wrong number / already uploaded
      - otherwise generate AI reply (context-aware) with portal link
      - weave in missing items naturally if any
      - auto-send if allowed (follow-up rules) else leave as draft
    """
    with _db() as db:
        m = db.execute("SELECT * FROM messages WHERE id=%s;", (message_id,)).fetchone()
        if not m or m["direction"] != "INBOUND":
            return

        c = db.execute("SELECT * FROM contacts WHERE id=%s;", (m["contact_id"],)).fetchone()
        if not c:
            return

        # 0) Determine missing (required+pending)
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
            (c["id"],),
        ).fetchall()
        missing_labels = [r["label"] for r in rows]

        # 1) Classify intent
        result = classify_inbound(m.get("body") or "")
        cat = (result.get("category") or "OTHER").upper()

        if cat == "DNC":
            db.execute("UPDATE contacts SET dnc=true WHERE id=%s;", (c["id"],))
            db.execute("INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','Client requested DNC');", (c["id"],))
            db.commit()
            return

        if cat == "WRONG_NUMBER":
            db.execute("UPDATE contacts SET phone=NULL WHERE id=%s;", (c["id"],))
            db.execute("INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','Wrong number reported');", (c["id"],))
            db.commit()
            return

        if cat == "ALREADY_UPLOADED":
            db.execute("INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'NOTE','Client says already uploaded');", (c["id"],))
            db.commit()
            return

        # 2) Build portal link & draft LLM reply
        portal = build_portal_url(db, c["id"], PORTAL_BASE)

        # Thread under last provider message if possible
        prev = _latest_provider_msgid_and_subject(db, c["id"])
        reply_to = (prev and prev["pmid"]) or None
        thread_subject = (prev and prev["subj"]) or "Regarding your case"
        subject = thread_subject if thread_subject.lower().startswith("re:") else f"Re: {thread_subject}"

        drafted = draft_ack_for_inbound(
            db,
            c,
            m.get("body") or "",
            portal,
            missing_labels=missing_labels,   # <-- weave in naturally
        )
        draft_id = drafted["id"]
        drafted_meta = drafted["meta"] or {}
        # add threading hints to the just-created draft
        db.execute(
            "UPDATE messages SET meta = COALESCE(meta,'{}'::jsonb) || %s::jsonb WHERE id=%s;",
            (Json({"reply_to_message_id": reply_to, "subject": subject}, dumps=_json_dumps), draft_id),
        )

        # 3) Auto-send decision (FOLLOW-UP rules)
        org = _org_settings(db)
        now_utc = datetime.now(timezone.utc)
        allowed, decision_meta, when = should_autosend(
            {
                "org": {
                    "require_approval_initial": org["require_approval_initial"],     # only gates initial, not follow-ups
                    "autosend_confidence_threshold": float(org["autosend_confidence_threshold"]),
                    "business_hours_tz": org["business_hours_tz"],
                    "business_hours_start": org["business_hours_start"],
                    "business_hours_end": org["business_hours_end"],
                    "cooldown_hours": org["cooldown_hours"],
                    "max_daily_sends": org["max_daily_sends"],
                    "grace_minutes": org["grace_minutes"],
                },
                "contact": {
                    "dnc": c["dnc"],
                    "last_sent_at": c["last_sent_at"],
                    "sends_today": c["sends_today"],
                },
                "drafted": drafted_meta,
                "is_initial": False,  # FOLLOW-UP
                "now_utc": now_utc,
            }
        )

        db.execute(
            "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'AUTO_SEND_DECISION',%s);",
            (c["id"], Json({"message_id": draft_id, **(decision_meta or {})}, dumps=_json_dumps)),
        )
        db.commit()

        if not allowed:
            return  # stay as draft for review

        # 4) Enqueue send (or schedule)
        q = _get_queue()
        try:
            if when and when > now_utc:
                try:
                    from rq.scheduler import Scheduler
                    scheduler = Scheduler(QUEUE_NAME, connection=q.connection)
                    print(f"[scheduler] enqueue_at queue={QUEUE_NAME} message_id={draft_id} when={when.isoformat()}")
                    scheduler.enqueue_at(when, "app.jobs.send_message_and_update", draft_id, "EMAIL")
                except Exception:
                    print(f"[queue] enqueue (fallback) send_message_and_update id={draft_id} q={QUEUE_NAME}")
                    q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
            else:
                print(f"[queue] enqueue send_message_and_update id={draft_id} q={QUEUE_NAME}")
                q.enqueue("app.jobs.send_message_and_update", draft_id, "EMAIL")
        except Exception:
            # if queue fails, leave as draft
            pass

# backend/app/routes_webhooks.py
from fastapi import APIRouter, Request, HTTPException, Depends, Form
from fastapi.responses import JSONResponse
from psycopg import Connection
from psycopg.types.json import Json
from app.deps import get_db
from app.queue import get_queue

from urllib.parse import parse_qs
from email.parser import BytesParser
from email.policy import default as email_default

import os
import re
import hmac
import hashlib
import base64
import html as htmllib
import json as pyjson
from uuid import UUID

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# -------------------------------------------------
# Config
# -------------------------------------------------
REPLIES_PREFIX = os.getenv("REPLIES_PREFIX", "r")   # e.g. "r"
REPLIES_DOMAIN = os.getenv("REPLIES_DOMAIN")        # optional (not strictly required to parse)

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _html_to_text(s: str) -> str:
    """Very light HTML → text conversion suitable for email bodies."""
    if not s:
        return ""
    s = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", "", s)
    s = re.sub(r"(?is)<br\s*/?>", "\n", s)
    s = re.sub(r"(?is)</p\s*>", "\n\n", s)
    s = re.sub(r"(?is)<li\s*>", "- ", s)
    s = re.sub(r"(?is)<[^>]+>", "", s)
    return htmllib.unescape(s).strip()

def _extract_contact_id(to_field: str) -> str | None:
    """
    Find r+<UUID>@domain in any 'To' header variant.
    We only care that the local-part starts with '<prefix>+' and the next 36 chars parse as a UUID.
    """
    if not to_field:
        return None
    for addr in re.findall(r"[\w\.\+\-]+@[\w\.\-]+", to_field):
        local, _, _domain = addr.partition("@")
        if "+" not in local:
            continue
        prefix, _, suffix = local.partition("+")
        if prefix != REPLIES_PREFIX:
            continue
        cid = suffix[:36]  # take first 36 chars after '+'
        try:
            UUID(cid)
            return cid
        except Exception:
            continue
    return None

def norm_phone(p: str | None) -> str | None:
    """Normalize US numbers to +1XXXXXXXXXX where possible."""
    if not p:
        return None
    digits = re.sub(r"\D", "", p)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if p.startswith("+"):
        return p
    return "+" + digits

def verify_twilio_signature(request: Request, body_bytes: bytes) -> bool:
    """
    Optional: verify X-Twilio-Signature. For local dev, skip if token missing.
    (For production, prefer Twilio's official RequestValidator.)
    """
    token = os.getenv("TWILIO_AUTH_TOKEN")
    sig = request.headers.get("X-Twilio-Signature")
    if not token or not sig:
        return True
    url = str(request.url)
    mac = hmac.new(token.encode("utf-8"), msg=(url.encode("utf-8") + body_bytes), digestmod=hashlib.sha1)
    expected = base64.b64encode(mac.digest()).decode("utf-8")
    return hmac.compare_digest(expected, sig)

def _extract_plain_text_from_form(form) -> str | None:
    """
    Robust body extraction for SendGrid Inbound Parse payloads:
    1) Prefer 'text'
    2) Otherwise downgrade 'html'
    3) Otherwise parse raw RFC822 in 'email' (UploadFile or bytes/str)
    Returns None if nothing usable is found.
    """
    # 1) 'text'
    text = form.get("text")
    if text is not None:
        if hasattr(text, "read"):  # UploadFile edge
            try:
                text = text.file.read().decode(errors="ignore")
            except Exception:
                text = ""
        elif isinstance(text, bytes):
            text = text.decode(errors="ignore")
        else:
            text = str(text)
        if text.strip():
            return text.strip()

    # 2) 'html' → text
    html_field = form.get("html")
    if html_field is not None:
        if hasattr(html_field, "read"):
            try:
                html_field = html_field.file.read().decode(errors="ignore")
            except Exception:
                html_field = ""
        elif isinstance(html_field, bytes):
            html_field = html_field.decode(errors="ignore")
        else:
            html_field = str(html_field)
        html_field = html_field.strip()
        if html_field:
            return _html_to_text(html_field)

    # 3) Raw MIME in 'email'
    raw = form.get("email")
    if raw is not None:
        try:
            if hasattr(raw, "read"):
                raw_bytes = raw.file.read()
            elif isinstance(raw, bytes):
                raw_bytes = raw
            else:
                raw_bytes = str(raw).encode()
            msg = BytesParser(policy=email_default).parsebytes(raw_bytes)
            if msg.is_multipart():
                # prefer text/plain part
                for part in msg.walk():
                    ctype = part.get_content_type().lower()
                    if ctype == "text/plain":
                        txt = part.get_content() or ""
                        if txt.strip():
                            return txt.strip()
                # fallback to text/html
                for part in msg.walk():
                    ctype = part.get_content_type().lower()
                    if ctype == "text/html":
                        html_txt = part.get_content() or ""
                        html_txt = _html_to_text(html_txt)
                        if html_txt.strip():
                            return html_txt.strip()
            else:
                ctype = msg.get_content_type().lower()
                if ctype == "text/plain":
                    txt = msg.get_content() or ""
                    if txt.strip():
                        return txt.strip()
                if ctype == "text/html":
                    html_txt = _html_to_text(msg.get_content() or "")
                    if html_txt.strip():
                        return html_txt.strip()
        except Exception:
            pass

    return None

# -------------------------------------------------
# Twilio SMS inbound
# -------------------------------------------------
@router.post("/twilio/sms")
async def twilio_sms(request: Request, db: Connection = Depends(get_db)):
    try:
        raw_bytes = await request.body()
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        form = parse_qs(raw_text, keep_blank_values=True)

        def first(key: str):
            v = form.get(key) or form.get(key.lower())
            return v[0] if v else None

        raw_from = first("From")
        raw_to   = first("To")
        raw_body = first("Body") or ""

        if not raw_from or not raw_to:
            raise HTTPException(status_code=400, detail=f"missing From/To: From={raw_from}, To={raw_to}")

        if not verify_twilio_signature(request, raw_bytes):
            raise HTTPException(status_code=403, detail="bad signature")

        from_phone = norm_phone(str(raw_from))
        to_phone   = norm_phone(str(raw_to))
        text       = str(raw_body).strip()

        if not from_phone:
            raise HTTPException(status_code=400, detail=f"invalid From phone: {raw_from}")

        row = db.execute("SELECT id FROM contacts WHERE phone = %s LIMIT 1;", (from_phone,)).fetchone()
        if row:
            contact_id = row["id"]
        else:
            contact_id = db.execute(
                "INSERT INTO contacts (first_name,last_name,email,phone,status) VALUES ('','',NULL,%s,'NEW') RETURNING id;",
                (from_phone,)
            ).fetchone()["id"]
            db.execute(
                "INSERT INTO timeline (contact_id,type,detail) VALUES (%s,'NOTE','Contact auto-created from inbound SMS');",
                (contact_id,)
            )

        db.execute(
            "INSERT INTO messages (contact_id, channel, direction, body, meta) VALUES (%s,'SMS','INBOUND',%s,%s);",
            (contact_id, text, Json({"from": from_phone, "to": to_phone}))
        )
        db.execute(
            "INSERT INTO timeline (contact_id,type,detail) VALUES (%s,'INBOUND','SMS received');",
            (contact_id,)
        )
        db.commit()

        return {"ok": True, "contact_id": contact_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/twilio/sms failed: {e}")

# -------------------------------------------------
# Dev-only email simulator (handy for quick tests)
# -------------------------------------------------
@router.post("/dev/email")
async def dev_email(
    db: Connection = Depends(get_db),
    to_email: str = Form(...),
    from_email: str = Form(...),
    body: str = Form("")
):
    row = db.execute(
        "SELECT id FROM contacts WHERE email = %s LIMIT 1;", (from_email,)
    ).fetchone()

    if row:
        contact_id = row["id"]
    else:
        contact_id = db.execute(
            """
            INSERT INTO contacts (first_name, last_name, email, phone, status)
            VALUES ('', '', %s, NULL, 'NEW')
            RETURNING id;
            """,
            (from_email,)
        ).fetchone()["id"]
        db.execute(
            "INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'NOTE','Contact auto-created from inbound email (DEV)');",
            (contact_id,)
        )

    db.execute(
        """
        INSERT INTO messages (contact_id, channel, direction, body, meta)
        VALUES (%s, 'EMAIL', 'INBOUND', %s, %s);
        """,
        (contact_id, body.strip(), Json({"from": from_email, "to": to_email}))
    )
    db.execute(
        "INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'INBOUND','Email received (DEV)');",
        (contact_id,)
    )
    db.commit()
    return {"ok": True, "contact_id": contact_id}

# -------------------------------------------------
# SendGrid Inbound Parse webhook (robust body extraction)
# -------------------------------------------------
@router.post("/sendgrid/inbound")
async def sendgrid_inbound(request: Request, db: Connection = Depends(get_db)):
    # Starlette parses multipart into FormData with strings and/or UploadFile objects
    form = await request.form()

    to_raw       = (form.get("to") or "").strip()
    from_raw     = (form.get("from") or "").strip()
    subject      = (form.get("subject") or "").strip()
    headers_raw  = (form.get("headers") or "")
    envelope_raw = (form.get("envelope") or "")

    # Resolve contact id from To / envelope
    contact_id = _extract_contact_id(to_raw)
    if not contact_id and envelope_raw:
        try:
            env = pyjson.loads(envelope_raw)
            tos = env.get("to") or []
            if isinstance(tos, list) and tos:
                contact_id = _extract_contact_id(", ".join(map(str, tos)))
        except Exception:
            pass

    if not contact_id:
        return JSONResponse({"detail": "no contact matched to="}, status_code=404)

    # Extract body text from any available field
    body_text = _extract_plain_text_from_form(form) or "[no content in message body]"

    # Insert inbound
    row = db.execute(
        """
        INSERT INTO messages(contact_id, channel, direction, body, meta)
        VALUES (%s,'EMAIL','INBOUND',%s,%s)
        RETURNING id;
        """,
        (
            contact_id,
            body_text,
            Json({
                "subject": subject,
                "from": from_raw,
                "to": to_raw,
                "headers": headers_raw,
                "envelope": envelope_raw,
                # Hints for debugging what we received
                "has_text": bool(form.get("text")),
                "has_html": bool(form.get("html")),
                "has_email_raw": bool(form.get("email")),
            })
        ),
    ).fetchone()

    db.execute(
        "INSERT INTO timeline(contact_id,type,detail) VALUES (%s,'INBOUND','Email received via SendGrid');",
        (contact_id,),
    )
    db.commit()

   # Kick off auto-reply/labeling worker (enqueue the callable directly)
    try:
        from app.jobs import react_to_inbound  # local import to avoid cycles at import time
        q = get_queue()
        job = q.enqueue(react_to_inbound, str(row["id"]))
        print(f"[webhooks] enqueued react_to_inbound job_id={job.id} for message {row['id']}")
        queued = True
        job_id = job.id
    except Exception as e:
        print(f"[webhooks] FAILED to enqueue react_to_inbound for message {row['id']}: {e}")
        queued = False
        job_id = None

    return {"ok": True, "message_id": str(row["id"]), "queued": queued, "job_id": job_id}

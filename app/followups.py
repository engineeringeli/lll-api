# app/followups.py
import os, json, re, traceback
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from uuid import UUID

from psycopg.types.json import Json  # safe JSON binding for Postgres

# ---- OpenAI client (runtime + type-only) ----
if TYPE_CHECKING:
    # only used for type checking; not imported at runtime
    from openai import OpenAI as OpenAIClient

try:
    # runtime import under a different name so we don't use it in type expressions
    from openai import OpenAI as _RuntimeOpenAI
    _OPENAI_OK = True
except Exception:
    _RuntimeOpenAI = None  # type: ignore
    _OPENAI_OK = False

_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.5"))

def _client() -> Optional["OpenAIClient"]:
    if not _OPENAI_OK or not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        assert _RuntimeOpenAI is not None
        return _RuntimeOpenAI()
    except Exception:
        return None

# ------------------------------------------------------------
# Shared helpers the rest of the app imports:
#  - _portal_url(db, contact_id, base)
#  - classify_inbound(...)
#  - draft_followup_for_missing(...)
#  - draft_ack_for_inbound(...)
#  - generate_initial_docs_request(...)
# ------------------------------------------------------------

def _org_settings(db) -> dict:
    row = db.execute("""
      SELECT
        COALESCE(require_approval_initial, true)      AS require_approval_initial,
        COALESCE(autosend_confidence_threshold, .85)  AS autosend_confidence_threshold,
        COALESCE(business_hours_tz,'America/Los_Angeles') AS business_hours_tz,
        COALESCE(business_hours_start,8)              AS business_hours_start,
        COALESCE(business_hours_end,18)               AS business_hours_end,
        COALESCE(cooldown_hours,22)                   AS cooldown_hours,
        COALESCE(max_daily_sends,2)                   AS max_daily_sends,
        COALESCE(grace_minutes,5)                     AS grace_minutes,
        COALESCE(outbound_from_name,'Law Firm')       AS outbound_from_name,
        COALESCE(include_signature,false)             AS include_signature,
        COALESCE(outbound_signature,'')               AS outbound_signature
      FROM org_settings LIMIT 1;
    """).fetchone()
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
    return dict(row)

def _ensure_token(db, contact_id: str, ttl_days: int = 30) -> str:
    tok = db.execute(
        "SELECT token FROM portal_tokens WHERE contact_id=%s AND (expires_at IS NULL OR expires_at>now()) LIMIT 1",
        (contact_id,)
    ).fetchone()
    if tok:
        return tok["token"]
    import secrets
    token = secrets.token_urlsafe(24)
    exp = datetime.now(timezone.utc) + timedelta(days=ttl_days)
    db.execute(
        "INSERT INTO portal_tokens(token, contact_id, expires_at) VALUES (%s,%s,%s)",
        (token, contact_id, exp)
    )
    return token

def _portal_url(db, contact_id: str, base: Optional[str] = None) -> str:
    base = (base or os.getenv("PORTAL_BASE") or "http://localhost:3000").rstrip("/")
    return f"{base}/portal/{_ensure_token(db, contact_id, 30)}"

def _signature_block(org: dict) -> str:
    if not org.get("include_signature"):
        return ""
    sig = (org.get("outbound_signature") or "").strip()
    return sig

def _json_dumps(obj: Any) -> str:
    """Safe JSON dump for psycopg Json(...), handling UUID/datetime, etc."""
    def _default(o: Any):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, set):
            return list(o)
        return str(o)
    return json.dumps(obj, default=_default)

def _llm_json(system: str, user: str, *, fallback: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ask the LLM to return JSON. If anything fails, return `fallback`.
    The JSON spec is simple and documented per-caller.
    """
    cli = _client()
    if not cli:
        return fallback

    try:
        resp = cli.chat.completions.create(
            model=_MODEL,
            temperature=_TEMPERATURE,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        data = json.loads(raw)
        # Pass back token usage for debugging/analytics if desired
        data["_llm"] = {
            "model": _MODEL,
            "finish_reason": getattr(resp.choices[0], "finish_reason", None),
            "prompt_tokens": getattr(resp.usage, "prompt_tokens", None) if hasattr(resp, "usage") else None,
            "completion_tokens": getattr(resp.usage, "completion_tokens", None) if hasattr(resp, "usage") else None,
        }
        return data
    except Exception:
        # print(traceback.format_exc())  # optionally log
        return fallback

def _normalize(text: str) -> str:
    # strip leftover placeholders like [Your Name]
    text = re.sub(r"\[[^\]]+\]", "", text)
    return text.strip()

def _append_signature_once(body: str, org: dict) -> str:
    """Append the configured signature exactly once at the end of the body."""
    sig = (org.get("include_signature") and (org.get("outbound_signature") or "").strip()) or ""
    b = (body or "").strip()
    if not sig:
        return b
    # if the body already ends with the exact signature, don't add it again
    if b.endswith(sig):
        return b
    return f"{b}\n\n{sig}".strip()

def _first_name(contact: Dict[str, Any]) -> str:
    return (contact.get("first_name") or "").strip()

def _condensed_list(labels: List[str], max_items: int = 4) -> str:
    """
    Use a short bullet list if there are only a few items.
    Otherwise condense naturally.
    """
    if not labels:
        return ""
    if len(labels) <= max_items:
        return "\n".join([f"• {l}" for l in labels])
    return f"{labels[0]}, {labels[1]}, {labels[2]} and {len(labels)-3} other item(s)"

# ------------------------------------------------------------
# INITIAL DOCS REQUEST (email draft)
# ------------------------------------------------------------
def generate_initial_docs_request(
    db,
    contact: dict,
    missing_labels: List[str],
    portal_url: str,
    org: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Returns: {subject:str, body:str, confidence:float, intent:str}
    Always uses LLM (fallback only if API fails).
    """
    org = org or _org_settings(db)
    first = (contact.get("first_name") or "").strip()
    context = {
        "contact_first": first,
        "missing_labels": missing_labels,
        "portal_url": portal_url,
        "from_name": org.get("outbound_from_name") or "Our team",
        "signature": _signature_block(org),
    }

    system = (
        "You are a precise but warm legal assistant writing emails for a law firm.\n"
        "Voice: concise, human, natural; no fluff; no marketing salesy tone.\n"
        "Constraints: 60–140 words. Use short paragraphs. If list is present, use bullets.\n"
        "Never invent facts. If there are 0 missing items, explain that the portal has the list.\n"
        'Return JSON: {"subject": str, "body": str}. No extra keys.'
    )
    user = json.dumps({
        "task": "compose_initial_documents_request",
        "contact_first": context["contact_first"] or None,
        "missing_labels": context["missing_labels"],
        "portal_url": context["portal_url"],
        "from_name": context["from_name"],
        "signature": context["signature"] or None
    })

    fb_body = []
    hi = f"Hi {first}," if first else "Hi,"
    if missing_labels:
        bullets = "\n".join([f"• {m}" for m in missing_labels])
        fb_body.append(hi)
        fb_body.append("")
        fb_body.append("To get started, please upload the following:")
        fb_body.append(bullets)
    else:
        fb_body.append(hi)
        fb_body.append("")
        fb_body.append("Here’s your secure link to upload documents related to your matter.")
    fb_body.append("")
    fb_body.append(f"Secure upload link: {portal_url}")
    fb = {
        "subject": "Documents needed — secure upload link",
        "body": "\n".join(fb_body).strip()
    }

    data = _llm_json(system, user, fallback=fb)
    body = _normalize(data.get("body") or fb["body"])
    # Attach signature (server-side) if configured and not already present
    sig = context["signature"]
    if sig and sig not in body:
        body = f"{body}\n\n{sig}".strip()

    return {
        "subject": _normalize(data.get("subject") or fb["subject"]),
        "body": body,
        "confidence": 0.98 if data is not fb else 0.75,
        "intent": "initial_docs_request",
        "_llm": data.get("_llm")
    }

# ------------------------------------------------------------
# FOLLOW-UP FOR MISSING DOCS
# ------------------------------------------------------------
def draft_followup_for_missing(db, contact_id: str, missing_labels: List[str], portal_url: str) -> str:
    """
    Creates and returns a draft message id for a follow-up email.
    Always uses LLM first; gracefully falls back.
    """
    c = db.execute("SELECT * FROM contacts WHERE id=%s;", (contact_id,)).fetchone()
    if not c:
        raise ValueError("contact not found")
    org = _org_settings(db)
    first = (c.get("first_name") or "").strip()

    bullet_block = _condensed_list(missing_labels)

    system = (
        "You are a concise, friendly paralegal. Remind a client about missing documents.\n"
        "Tone: polite, helpful, normal human. 60–110 words. If there are ≤4 items, include a short bullet list; "
        "if there are more, name a few and mention there are others.\n"
        'Return JSON: {"body": str}.'
    )
    user = json.dumps({
        "task": "followup_missing_documents",
        "contact_first": first or None,
        "missing_labels": missing_labels,
        "portal_url": portal_url,
        "from_name": org.get("outbound_from_name"),
        "signature": _signature_block(org) or None,
    })

    hi = f"Hi {first}," if first else "Hi,"
    fb_list = f"Here’s a quick list:\n{bullet_block}\n\n" if bullet_block else ""
    fb = {
        "body": _normalize(
            f"{hi}\n\nJust checking in — we still need a few documents to keep things moving.\n"
            f"{fb_list}"
            f"You can upload them here: {portal_url}\n\n"
            f"Thank you!"
        )
    }

    data = _llm_json(system, user, fallback=fb)
    body = _normalize(data.get("body") or fb["body"])
    sig = _signature_block(org)
    if sig and sig not in body:
        body = f"{body}\n\n{sig}".strip()

    # Save draft
    meta = {
        "intent": "doc_followup",
        "missing_labels": missing_labels,
        "portal": portal_url,
        "confidence": 0.98 if data is not fb else 0.75,
        "_llm": data.get("_llm"),
    }
    r = db.execute(
        "INSERT INTO messages(contact_id, channel, direction, body, meta) VALUES (%s,'EMAIL','DRAFT',%s,%s) RETURNING id;",
        (contact_id, body, Json(meta, dumps=_json_dumps)),
    ).fetchone()
    return r["id"]

# ------------------------------------------------------------
# ACK / REPLY FOR INBOUND (context-aware, gently mentions missing)
# ------------------------------------------------------------
def draft_ack_for_inbound(
    db,
    contact: dict,
    inbound_text: str,
    portal_url: str,
    *,
    missing_labels: Optional[List[str]] = None,   # <-- backwards compatible
) -> Dict[str, Any]:
    """
    Builds a short, contextful reply referencing the client’s inbound message.
    If missing_labels are provided (and non-empty), weave them in naturally:
      - ≤4 items: short bullet list
      - >4 items: name a few + mention count
      - or add a brief P.S. if the inbound looks about something else
    Returns {id, meta}.
    """
    org = _org_settings(db)
    first = _first_name(contact)
    missing_labels = missing_labels or []

    bullet_block = _condensed_list(missing_labels)
    looks_about_docs = any(
        w in (inbound_text or "").lower()
        for w in ("doc", "document", "upload", "files", "send over", "requirements", "what do you need")
    )

    greeting = f"Hi {first}," if first else "Hi,"

    if missing_labels:
        if looks_about_docs or len(missing_labels) <= 4:
            mention = f"Here’s what would be most helpful next:\n{bullet_block}\n\n"
        else:
            mention = f"P.S. We’re still missing a few items (e.g., {bullet_block}). " \
                      f"You can upload them here: {portal_url}\n\n"
    else:
        mention = ""

    system = (
        "You are a legal assistant writing short, natural email replies. "
        "Sound human, warm, and specific to the user’s message. Avoid robotic phrasing. "
        "Use line breaks and light formatting for readability.\n"
        'Return JSON: {"body": str}.'
    )
    user = json.dumps({
        "task": "ack_inbound",
        "contact_first": first or None,
        "inbound_excerpt": (inbound_text or "")[-1500:],  # keep prompt sane
        "portal_url": portal_url,
        "mention_block": mention or None,
        "signature": _signature_block(org) or None,
    })

    fb = {
        "body": _normalize(
            f"{greeting}\n\nThanks for your message — happy to help.\n\n"
            f"{mention}"
            f"You can use this secure link any time: {portal_url}"
        )
    }

    data = _llm_json(system, user, fallback=fb)
    body = _normalize(data.get("body") or fb["body"])
    sig = _signature_block(org)
    if sig and sig not in body:
        body = f"{body}\n\n{sig}".strip()

    meta = {
        "intent": "inbound_ack",
        "missing_labels": missing_labels,
        "portal": portal_url,
        "confidence": 0.98 if data is not fb else 0.75,
        "_llm": data.get("_llm"),
    }
    r = db.execute(
        "INSERT INTO messages(contact_id, channel, direction, body, meta) VALUES (%s,'EMAIL','DRAFT',%s,%s) RETURNING id;",
        (contact["id"], body, Json(meta, dumps=_json_dumps)),
    ).fetchone()
    return {"id": r["id"], "meta": meta}

# ------------------------------------------------------------
# CLASSIFY INBOUND (DNC / WRONG_NUMBER / ALREADY_UPLOADED / OTHER)
# ------------------------------------------------------------
def classify_inbound(inbound_text: str) -> Dict[str, Any]:
    """
    Uses LLM to classify basic routing categories. Falls back to regex if API is down.
    """
    system = (
        "Classify the user message into one of: DNC, WRONG_NUMBER, ALREADY_UPLOADED, OTHER.\n"
        'Return JSON: {"category": "..." } only.'
    )
    user = json.dumps({"message": (inbound_text or "")[-2000:]})

    # simple fallback
    text = (inbound_text or "").lower()
    fb = {"category": "OTHER"}
    if any(k in text for k in ["stop", "unsubscribe", "do not contact", "dnc"]):
        fb = {"category": "DNC"}
    elif any(k in text for k in ["wrong number", "not my number", "who is this"]):
        fb = {"category": "WRONG_NUMBER"}
    elif any(k in text for k in ["i already uploaded", "i sent it", "i submitted"]):
        fb = {"category": "ALREADY_UPLOADED"}

    data = _llm_json(system, user, fallback=fb)
    return {"category": (data.get("category") or "OTHER").upper()}

# re-exported for other modules
_portal_url = _portal_url
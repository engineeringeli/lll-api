from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, EmailStr, field_validator
from psycopg import Connection
from psycopg.types.json import Json
from app.deps import get_db
from app.routes_webhooks import norm_phone  # reuse normalizer
from datetime import datetime

router = APIRouter(prefix="/leads", tags=["leads"])

class LeadIn(BaseModel):
    first_name: str = ""
    last_name: str = ""
    email: EmailStr | None = None
    phone: str | None = None
    matter_type: str = "general"
    source: str = "web"
    honeypot: str | None = None  # hidden field to deter bots

    @field_validator("first_name", "last_name", "matter_type", "source")
    @classmethod
    def trim(cls, v: str) -> str:
        return (v or "").strip()

@router.post("")
def create_lead(lead: LeadIn, db: Connection = Depends(get_db), request: Request = None):
    # 1) Honeypot: if filled, quietly accept but do nothing
    if lead.honeypot and lead.honeypot.strip():
        return {"ok": True, "ignored": True}

    # 2) Basic requirement: email or phone
    if not lead.email and not lead.phone:
        raise HTTPException(400, "Provide at least email or phone")

    phone_norm = norm_phone(lead.phone) if lead.phone else None

    # 3) Try to find existing contact by email or phone
    row = None
    if lead.email:
        row = db.execute("SELECT * FROM contacts WHERE email = %s LIMIT 1;", (lead.email,)).fetchone()
    if not row and phone_norm:
        row = db.execute("SELECT * FROM contacts WHERE phone = %s LIMIT 1;", (phone_norm,)).fetchone()

    # 4) Upsert logic
    if row:
        contact_id = row["id"]
        db.execute("""
            UPDATE contacts SET
              first_name = COALESCE(NULLIF(%s,''), first_name),
              last_name  = COALESCE(NULLIF(%s,''), last_name),
              email      = COALESCE(%s, email),
              phone      = COALESCE(%s, phone),
              matter_type= COALESCE(NULLIF(%s,''), matter_type),
              updated_at = NOW()
            WHERE id = %s;
        """, (lead.first_name, lead.last_name, lead.email, phone_norm, lead.matter_type, contact_id))
        db.execute("INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'NOTE','Lead info updated via web form');", (contact_id,))
    else:
        contact_id = db.execute("""
            INSERT INTO contacts (first_name, last_name, email, phone, status, matter_type)
            VALUES (%s,%s,%s,%s,'NEW',%s) RETURNING id;
        """, (lead.first_name, lead.last_name, lead.email, phone_norm, lead.matter_type)).fetchone()["id"]
        db.execute("INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'NOTE','Lead created via web form');", (contact_id,))

    # 5) Optional: capture lead meta
    client_ip = request.client.host if request and request.client else None
    meta = {"source": lead.source, "ip": client_ip, "ts": datetime.utcnow().isoformat() + "Z"}
    db.execute("INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'META',%s);", (contact_id, str(meta)))

    # -----------------------------
    # STEP 5.3 — AUTO-CREATE DRAFT
    # -----------------------------
  #  CALENDLY_URL = "https://calendly.com/yourlink/15min"  # <<< PLACEHOLDER: replace with your real link
# first = (lead.first_name or "").strip()
  #  matter = (lead.matter_type or "your legal matter").strip()

   # body = (
   #     f"Hi {first}, thanks for reaching out about {matter}. "
   #     f"A brief consult can clarify options. You can pick a time here: {CALENDLY_URL} "
   #     f"(Informational only; not legal advice.)"
   # ).strip()

   # msg_id = db.execute(
   #     "INSERT INTO messages (contact_id, channel, direction, body, meta) "
   #     "VALUES (%s,'EMAIL','DRAFT',%s,%s) RETURNING id;",
   #     (contact_id, body, Json({"intent": "book_consult", "template_id": "lead_auto_draft_v1"}))
   # ).fetchone()["id"]

   # db.execute(
   #     "INSERT INTO timeline (contact_id, type, detail) VALUES (%s,'NOTE','Auto-draft created from lead');",
   #     (contact_id,)
   # )
    # -----------------------------

  #  db.commit()
  #  return {"ok": True, "contact_id": contact_id}

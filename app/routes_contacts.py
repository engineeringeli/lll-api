# backend/app/routes_contacts.py
from fastapi import APIRouter, Depends, HTTPException, Body
from psycopg import Connection
from datetime import datetime, timezone
from app.deps import get_db

# reuse your docs-request creator from routes_messages
from app.routes_messages import draft_initial as draft_initial_docs

router = APIRouter(prefix="/contacts", tags=["contacts"])

# -------------------------------------------------------------------
# List contacts (used by Inbox)
# -------------------------------------------------------------------
@router.get("")
def list_contacts(db: Connection = Depends(get_db)):
    rows = db.execute(
        """
        SELECT id, first_name, last_name, email, phone, status
        FROM contacts
        ORDER BY updated_at DESC, created_at DESC;
        """
    ).fetchall()
    return rows

# Optional: fetch a single contact
@router.get("/{contact_id}")
def get_contact(contact_id: str, db: Connection = Depends(get_db)):
    r = db.execute(
        "SELECT id, first_name, last_name, email, phone, matter_type, status FROM contacts WHERE id=%s;",
        (contact_id,),
    ).fetchone()
    if not r:
        raise HTTPException(404, "contact not found")
    return r

# -------------------------------------------------------------------
# Create contact (NO “book a meeting” auto-draft)
# Optionally: set payload.draft_docs = true to immediately create a
# docs-request draft using your new flow.
# -------------------------------------------------------------------
@router.post("")
def create_contact(payload: dict = Body(...), db: Connection = Depends(get_db)):
    first = (payload.get("first_name") or "").strip() or None
    last  = (payload.get("last_name")  or "").strip() or None
    email = (payload.get("email")      or "").strip() or None
    phone = (payload.get("phone")      or "").strip() or None
    matter= (payload.get("matter_type")or "").strip() or None
    draft_docs = bool(payload.get("draft_docs", False))

    if not (email or phone):
        raise HTTPException(400, "email or phone required")

    row = db.execute(
        """
        INSERT INTO contacts (first_name, last_name, email, phone, matter_type, status, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,'NEW', now(), now())
        RETURNING id;
        """,
        (first, last, email, phone, matter),
    ).fetchone()
    contact_id = str(row["id"])
    db.commit()

    result = {"ok": True, "id": contact_id}

    # Optionally create the initial docs-request draft right now
    if draft_docs:
        try:
            draft_resp = draft_initial_docs(contact_id, {}, db)  # call your existing route function
            result["draft"] = draft_resp
        except Exception as e:
            # Do not fail the contact creation if drafting errors out
            result["draft_error"] = str(e)

    return result
# backend/app/routes_contacts.py
from fastapi import APIRouter, Depends, HTTPException, Body
from psycopg import Connection
from typing import Optional, Dict, Any
from app.deps import get_db

# If your existing function must be reused:
# It should accept (contact_id: str, payload: dict, db: Connection)
from app.routes_messages import draft_initial as draft_initial_docs

router = APIRouter(prefix="/contacts", tags=["contacts"])

# -------------------------------------------------------------------
# List contacts (used by Inbox) - keep result set bounded
# -------------------------------------------------------------------
@router.get("")
def list_contacts(limit: int = 200, db: Connection = Depends(db_conn)):
    # cap limit to something sane
    limit = max(1, min(limit, 500))
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT id, first_name, last_name, email, phone, status
            FROM public.contacts
            ORDER BY updated_at DESC, created_at DESC
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
    # rows are dicts if you set row_factory=dict_row in deps.py
    return rows

# -------------------------------------------------------------------
# Fetch a single contact
# -------------------------------------------------------------------
@router.get("/{contact_id}")
def get_contact(contact_id: str, db: Connection = Depends(db_conn)):
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT id, first_name, last_name, email, phone, matter_type, status
            FROM public.contacts
            WHERE id = %s;
            """,
            (contact_id,),
        )
        r = cur.fetchone()
    if not r:
        raise HTTPException(404, "contact not found")
    return r

# -------------------------------------------------------------------
# Create contact
# Optional: payload.draft_docs = true will immediately create a
# docs-request draft via a NEW short-lived connection, so we don't
# hold the request's connection longer than needed.
# -------------------------------------------------------------------
@router.post("")
def create_contact(payload: Dict[str, Any] = Body(...), db: Connection = Depends(db_conn)):
    first = (payload.get("first_name") or "").strip() or None
    last  = (payload.get("last_name")  or "").strip() or None
    email = (payload.get("email")      or "").strip() or None
    phone = (payload.get("phone")      or "").strip() or None
    matter= (payload.get("matter_type")or "").strip() or None
    draft_docs: bool = bool(payload.get("draft_docs", False))

    if not (email or phone):
        raise HTTPException(400, "email or phone required")

    try:
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.contacts
                  (first_name, last_name, email, phone, matter_type, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'NEW', now(), now())
                RETURNING id;
                """,
                (first, last, email, phone, matter),
            )
            row = cur.fetchone()
        db.commit()
    except Exception as e:
        # Always rollback on failure so the connection returns clean
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(500, f"failed to create contact: {e}")

    contact_id = str(row["id"])
    result: Dict[str, Any] = {"ok": True, "id": contact_id}

    # --- Optional follow-up: do NOT reuse the same connection
    if draft_docs:
        try:
            # Borrow a new connection briefly just for the draft call
            # Import here to avoid circulars if needed
            from app.deps import _get_pool  # this is in the updated deps.py I gave you
            pool = _get_pool()
            with pool.connection() as conn:
                draft_resp = draft_initial_docs(contact_id, {}, conn)
            result["draft"] = draft_resp
        except Exception as e:
            # Never fail the create endpoint because the draft failed
            result["draft_error"] = str(e)

    return result

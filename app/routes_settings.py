# backend/app/routes_settings.py
from fastapi import APIRouter, Depends, HTTPException, Body
from psycopg import Connection
from psycopg.types.json import Json
from app.deps import db_conn
from app.models import OrgSettingsOut, OrgSettingsUpdate

router = APIRouter(prefix="/org", tags=["org"])

BASE_FIELDS = [
    "require_approval_initial",
    "autosend_confidence_threshold",
    "business_hours_tz",
    "business_hours_start",
    "business_hours_end",
    "cooldown_hours",
    "max_daily_sends",
    "grace_minutes",
]

EXTRA_FIELDS = [
    "outbound_from_name",
    "outbound_signature",
    "include_signature",
]

ALL_FIELDS = BASE_FIELDS + EXTRA_FIELDS

@router.get("/settings")
def get_settings(db: Connection = Depends(db_conn)):
    row = db.execute("SELECT * FROM org_settings LIMIT 1;").fetchone()
    if not row:
        raise HTTPException(500, "org_settings row not found")
    out = dict(row)
    # cast floats explicitly
    if "autosend_confidence_threshold" in out and out["autosend_confidence_threshold"] is not None:
        out["autosend_confidence_threshold"] = float(out["autosend_confidence_threshold"])
    return {k: out.get(k) for k in ALL_FIELDS}

@router.api_route("/settings", methods=["PUT","POST"])
def update_settings(payload: dict = Body(...), db: Connection = Depends(db_conn)):
    sets, vals = [], []
    for k in ALL_FIELDS:
        if k in payload and payload[k] is not None:
            sets.append(f"{k} = %s")
            vals.append(payload[k])

    if sets:
        db.execute(f"UPDATE org_settings SET {', '.join(sets)};", tuple(vals))
        db.commit()

    row = db.execute("SELECT * FROM org_settings LIMIT 1;").fetchone()
    if not row:
        raise HTTPException(500, "org_settings row not found")
    out = dict(row)
    if "autosend_confidence_threshold" in out and out["autosend_confidence_threshold"] is not None:
        out["autosend_confidence_threshold"] = float(out["autosend_confidence_threshold"])
    return {k: out.get(k) for k in ALL_FIELDS}

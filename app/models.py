# backend/app/models.py
from pydantic import BaseModel

class OrgSettingsOut(BaseModel):
    require_approval_initial: bool
    autosend_confidence_threshold: float
    business_hours_tz: str
    business_hours_start: int
    business_hours_end: int
    cooldown_hours: int
    max_daily_sends: int
    grace_minutes: int

class OrgSettingsUpdate(BaseModel):
    require_approval_initial: bool | None = None
    autosend_confidence_threshold: float | None = None
    business_hours_tz: str | None = None
    business_hours_start: int | None = None
    business_hours_end: int | None = None
    cooldown_hours: int | None = None
    max_daily_sends: int | None = None
    grace_minutes: int | None = None

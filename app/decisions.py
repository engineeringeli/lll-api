# backend/app/decisions.py
from datetime import datetime, timedelta
import pytz

def within_business_hours(now_local: datetime, start_h: int, end_h: int) -> bool:
    return start_h <= now_local.hour < end_h

def should_autosend(ctx: dict):
    """
    ctx:
      - org: dict(org settings)
      - contact: dict(dnc: bool, last_sent_at: datetime|None, sends_today: int)
      - drafted: dict(compliance_ok: bool, confidence: float, template_id: str)
      - is_initial: bool
      - now_utc: aware datetime (UTC)
    Returns: (allowed: bool, meta: dict, when_to_send_utc: datetime|None)
    """
    s = ctx["org"]
    c = ctx["contact"]
    m = ctx["drafted"]
    now_utc = ctx["now_utc"]

    reasons = []

    if c.get("dnc"):
        reasons.append("contact_on_dnc")
        return False, {"reasons": reasons}, None

    if c.get("sends_today", 0) >= s["max_daily_sends"]:
        reasons.append("daily_limit_reached")
        return False, {"reasons": reasons}, None

    if c.get("last_sent_at"):
        delta = now_utc - c["last_sent_at"]
        if delta.total_seconds() < s["cooldown_hours"] * 3600:
            reasons.append("cooldown_active")
            return False, {"reasons": reasons}, None

    if not m.get("compliance_ok", False):
        reasons.append("compliance_failed")
        return False, {"reasons": reasons}, None

    if float(m.get("confidence", 0.0)) < float(s["autosend_confidence_threshold"]):
        reasons.append("confidence_below_threshold")
        return False, {"reasons": reasons}, None

    if s.get("require_approval_initial", True) and ctx.get("is_initial", True):
        reasons.append("approval_required_by_policy")
        return False, {"reasons": reasons}, None

    # Business hours scheduling
    tz = pytz.timezone(s["business_hours_tz"])
    local_now = now_utc.astimezone(tz)
    if not within_business_hours(local_now, s["business_hours_start"], s["business_hours_end"]):
        send_time = local_now.replace(hour=s["business_hours_start"], minute=0, second=0, microsecond=0)
        if local_now.hour >= s["business_hours_end"]:
            send_time = send_time + timedelta(days=1)
        reasons.append("scheduled_for_business_hours")
        return True, {"reasons": reasons}, send_time.astimezone(pytz.utc)

    when = now_utc + timedelta(minutes=s.get("grace_minutes", 0))
    if s.get("grace_minutes", 0) > 0:
        reasons.append("grace_period")
    else:
        reasons.append("immediate_autosend")

    return True, {"reasons": reasons}, when

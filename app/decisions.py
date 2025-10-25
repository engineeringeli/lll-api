# backend/app/decisions.py
import os
from datetime import datetime, timedelta, timezone
import pytz

# Treat these as truthy: 1/true/yes/on (case-insensitive)
_TRUEY = {"1", "true", "yes", "on"}


def _env_true(name: str, default: str = "") -> bool:
    return str(os.getenv(name, default)).strip().lower() in _TRUEY


def within_business_hours(now_local: datetime, start_h: int, end_h: int) -> bool:
    return start_h <= now_local.hour < end_h


def should_autosend(ctx: dict):
    """
    Decision engine for auto-send.

    ctx:
      - org: dict of org settings (keys used here are optional)
          require_approval_initial: bool
          autosend_confidence_threshold: float
          business_hours_tz: str
          business_hours_start: int
          business_hours_end: int
          cooldown_hours: float
          max_daily_sends: int
          grace_minutes: int
          autosend_all_followups: bool  # optional feature flag in DB
      - contact: dict (dnc: bool, last_sent_at: datetime|None, sends_today: int)
      - drafted: dict (confidence: float, compliance_ok: bool [optional])
      - is_initial: bool
      - now_utc: aware datetime (UTC)

    Returns: (allowed: bool, meta: dict, when_to_send_utc: datetime|None)
      - If allowed==True and when is None -> send immediately
      - If allowed==True and when > now -> schedule for 'when'
      - If allowed==False -> leave as draft
    """
    s = (ctx.get("org") or {})
    c = (ctx.get("contact") or {})
    m = (ctx.get("drafted") or {})
    now_utc = ctx.get("now_utc") or datetime.now(timezone.utc)
    is_initial = bool(ctx.get("is_initial"))
    reasons = []

    # ---- Hard stop: DNC always blocks ----
    if c.get("dnc"):
        reasons.append("contact_on_dnc")
        return False, {"reasons": reasons}, None

    # ---- Global overrides (env) / org flags ----
    # Auto-send ALL follow-ups immediately (ignore business hours/cooldown/etc.)
    if (not is_initial) and (
        _env_true("AUTOSEND_FOLLOWUPS_ALWAYS") or bool(s.get("autosend_all_followups"))
    ):
        reasons.append("force_followup_autosend")
        # Immediate: return when=None so callers enqueue now
        return True, {"reasons": reasons}, None

    # Auto-send initial if env flag is set OR org doesn't require approval
    if is_initial and (_env_true("AUTOSEND_INITIAL_ALWAYS") or not bool(s.get("require_approval_initial", True))):
        reasons.append("initial_no_approval")
        return True, {"reasons": reasons}, None

    # ---- Daily limit ----
    max_daily = int(s.get("max_daily_sends", 2) or 0)
    if int(c.get("sends_today") or 0) >= max_daily > 0:
        reasons.append("daily_limit_reached")
        return False, {"reasons": reasons}, None

    # ---- Cooldown window ----
    cool_hours = float(s.get("cooldown_hours", 22) or 0.0)
    last_sent_at = c.get("last_sent_at")
    if last_sent_at and cool_hours > 0:
        delta = now_utc - last_sent_at
        if delta.total_seconds() < cool_hours * 3600:
            reasons.append("cooldown_active")
            return False, {"reasons": reasons}, None

    # ---- Compliance gate (only blocks if explicitly False) ----
    # If your drafting code doesn't set compliance_ok, we treat it as OK.
    if m.get("compliance_ok") is False:
        reasons.append("compliance_failed")
        return False, {"reasons": reasons}, None

    # ---- Confidence threshold ----
    # Default confidence to 1.0 so absence doesn't block autosend.
    threshold = float(s.get("autosend_confidence_threshold", 0.85))
    confidence = float(m.get("confidence", 1.0))
    if confidence < threshold:
        reasons.append("confidence_below_threshold")
        return False, {"reasons": reasons}, None

    # ---- Business-hours scheduling ----
    tzname = s.get("business_hours_tz", "America/Los_Angeles")
    start_h = int(s.get("business_hours_start", 8))
    end_h = int(s.get("business_hours_end", 18))
    tz = pytz.timezone(tzname)
    local_now = now_utc.astimezone(tz)
    if not within_business_hours(local_now, start_h, end_h):
        # Schedule for next opening window
        send_time = local_now.replace(hour=start_h, minute=0, second=0, microsecond=0)
        if local_now.hour >= end_h:
            send_time = send_time + timedelta(days=1)
        reasons.append("scheduled_for_business_hours")
        return True, {"reasons": reasons}, send_time.astimezone(pytz.utc)

    # ---- Optional grace period before immediate send ----
    grace_min = int(s.get("grace_minutes", 0) or 0)
    when = (now_utc + timedelta(minutes=grace_min)) if grace_min > 0 else None
    reasons.append("grace_period" if grace_min > 0 else "immediate_autosend")

    return True, {"reasons": reasons}, when
# backend/app/queue.py
import os
from redis import from_url
from rq import Queue

# Single source of truth for queue name
QUEUE_NAME = os.getenv("QUEUE_NAME", "outbound")

def get_connection():
    raw = os.getenv("REDIS_URL") or ""
    url = "".join(raw.split())
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    # Note: use decode_responses=False for RQ/Redis binary safety
    return from_url(url, decode_responses=False)

def get_queue() -> Queue:
    return Queue(QUEUE_NAME, connection=get_connection(), default_timeout=120)

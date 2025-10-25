# backend/app/queue.py
import os
from redis import from_url
from rq import Queue
from dotenv import load_dotenv

# Ensure .env values override any stale shell exports
load_dotenv(override=True)

def get_queue() -> Queue:
    raw = os.getenv("REDIS_URL") or ""
    # remove ALL whitespace just in case (spaces, tabs, newlines, NBSP)
    url = "".join(raw.split())
    if not url:
        raise RuntimeError("REDIS_URL is not set")
    # rediss:// scheme automatically enables TLS; certs are handled
    # by Python's SSL (and you set SSL_CERT_FILE in .env already)
    redis = from_url(url, decode_responses=True)
    return Queue("outbound", connection=redis)

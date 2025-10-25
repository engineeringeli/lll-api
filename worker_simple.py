# backend/worker_simple.py
import os, sys
import sys, os
sys.path.insert(0, os.path.dirname(__file__))  # ensure /backend is on sys.path
import app.jobs  # makes functions importable for RQ
from pathlib import Path
from dotenv import load_dotenv

# --- Load envs (backend/.env) ---
BASE_DIR = Path(__file__).resolve().parent  # .../backend
load_dotenv(dotenv_path=BASE_DIR / ".env", override=True)

# --- Ensure 'app' is importable (for "app.jobs.*" string jobs) ---
# Add backend/ to sys.path so "import app" works
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Optional: verify we can import app.jobs now (prints once at start)
try:
    import app.jobs  # noqa: F401
except Exception as e:
    print("[worker] Could not import app.jobs. sys.path is:")
    for p in sys.path:
        print("   ", p)
    raise

# --- Redis / RQ setup ---
import redis
from rq import SimpleWorker, Queue

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL not set in environment")

# Works with rediss:// (TLS)
rconn = redis.from_url(REDIS_URL, decode_responses=False)

# Fail fast if bad URL / network
try:
    rconn.ping()
    print("[worker] Connected to Redis.")
except Exception as e:
    raise RuntimeError(f"[worker] Redis connection failed: {e}")

# Outbound queue only (no forking -> mac-safe)
q = Queue("outbound", connection=rconn)

if __name__ == "__main__":
    w = SimpleWorker([q], connection=rconn)
    print("[worker] Listening on 'outbound'…")
    w.work(burst=False)  # keep running
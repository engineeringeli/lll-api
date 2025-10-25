# backend/app/storage.py
import os

# PLACEHOLDERS you must set in backend/.env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")
BUCKET = os.getenv("UPLOADS_BUCKET", "client-uploads")

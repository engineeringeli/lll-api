# backend/app/deps.py
import os, redis, psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

def get_db():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn

def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)

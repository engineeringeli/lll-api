# backend/app/routes_debug.py
from fastapi import APIRouter, Depends
from psycopg import Connection
from app.deps import db_conn

router = APIRouter(prefix="/debug", tags=["debug"])

@router.get("/db")
def debug_db(db: Connection = Depends(db_conn)):
    info = db.execute("""
        select
          current_user,
          current_database() as db,
          inet_server_addr()::text as host,
          inet_server_port() as port,
          version()
    """).fetchone()
    cnt = db.execute("select count(*) as n from contacts;").fetchone()["n"]
    return {"db": info, "contacts_count": cnt}

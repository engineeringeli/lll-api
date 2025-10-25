import os, psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()
DB = os.getenv("DATABASE_URL")
leads = [
  ("Alicia","Nguyen","alicia@example.com","+17025550101","IRS Notice - CP2000"),
  ("Marco","Reyes","marco@example.com","+17025550102","Back Taxes (3 yrs)"),
  ("Dana","Lee","dana@example.com","+17025550103","Audit Letter"),
]

with psycopg.connect(DB, row_factory=dict_row) as conn:
    for f,l,e,p,m in leads:
        conn.execute("""
          insert into contacts (first_name,last_name,email,phone,matter_type)
          values (%s,%s,%s,%s,%s)
        """,(f,l,e,p,m))
    conn.commit()
print("Seeded", len(leads), "contacts")

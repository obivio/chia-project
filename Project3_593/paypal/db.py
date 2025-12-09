import sqlite3, os, time

DB_PATH = "paypal.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  billing_address TEXT NOT NULL,
  item TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  created REAL NOT NULL
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.commit()

def insert_payment(user_id: str, billing_address: str,
                   item: str, amount_cents: int) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO payments(user_id,billing_address,item,amount_cents,created) "
            "VALUES(?,?,?,?,?)",
            (user_id, billing_address, item, amount_cents, time.time()),
        )
        conn.commit()
        return cur.lastrowid

def delete_payments_by_user(user_id: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM payments WHERE user_id=?",
            (user_id,),
        )
        conn.commit()
        return cur.rowcount

import sqlite3, os, time

DB_PATH = "pencilpros.db"

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS purchases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  item TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  created REAL NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
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

def upsert_user(user_id: str, name: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO users(user_id,name) VALUES(?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET name=excluded.name",
            (user_id, name),
        )
        conn.commit()

def insert_purchase(user_id: str, item: str, amount_cents: int) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO purchases(user_id,item,amount_cents,created) "
            "VALUES(?,?,?,?)",
            (user_id, item, amount_cents, time.time()),
        )
        conn.commit()
        return cur.lastrowid

def delete_user_and_purchases(user_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM purchases WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE user_id=?", (user_id,))
        conn.commit()

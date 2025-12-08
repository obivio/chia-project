import sqlite3, time, json, hashlib, os
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS provenance (
  event_id TEXT PRIMARY KEY,
  t_unix REAL NOT NULL,
  op TEXT NOT NULL,          -- 'source','transfer_out','transfer_in','delete_request','delete_done'
  src_app TEXT NOT NULL,
  dst_app TEXT,
  user_id TEXT NOT NULL,
  tag_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  meta TEXT NOT NULL
);
"""

class ProvLogger:
    def __init__(self, db_path: str, appname: str):
        self.db_path = db_path
        self.appname = appname
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        with sqlite3.connect(self.db_path) as c:
            c.execute(SCHEMA)
            c.commit()

    def log(self, op: str, user_id: str, tag_id: str, payload: bytes,
            dst_app: Optional[str] = None, meta: Optional[dict] = None):
        meta = meta or {}
        event_id = hashlib.sha256(
            f"{time.time()}|{op}|{user_id}|{tag_id}".encode()
        ).hexdigest()
        phash = hashlib.sha256(payload).hexdigest()
        rec = (
            event_id,
            time.time(),
            op,
            self.appname,
            dst_app,
            user_id,
            tag_id,
            phash,
            json.dumps(meta),
        )
        with sqlite3.connect(self.db_path) as c:
            c.execute(
                """INSERT INTO provenance(event_id,t_unix,op,src_app,dst_app,user_id,tag_id,payload_hash,meta)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                rec,
            )
            c.commit()
        return event_id

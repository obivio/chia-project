# mini_runtime.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, Callable, Optional
from contextvars import ContextVar
import sqlite3, time, json, uuid, hashlib, os

# ---------- Labels & Labeled wrapper ----------
T = TypeVar("T")
current_user: ContextVar[str | None] = ContextVar("current_user", default=None)

@dataclass(frozen=True)
class Label:
    user_id: str
    tag_id: str
    policies: dict

    def to_header(self) -> str:
        return json.dumps({"user_id": self.user_id, "tag_id": self.tag_id, "policies": self.policies})

    @staticmethod
    def from_header(s: str) -> "Label":
        d = json.loads(s)
        return Label(d["user_id"], d["tag_id"], d["policies"])

def new_label(user_id: str, policies: dict) -> Label:
    return Label(user_id=user_id, tag_id=str(uuid.uuid4()), policies=policies)

@dataclass
class Labeled(Generic[T]):
    value: T
    label: Label

# ---------- Provenance logger ----------
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
            dst_app: Optional[str]=None, meta: Optional[dict]=None):
        meta = meta or {}
        event_id = hashlib.sha256(f"{time.time()}|{op}|{user_id}|{tag_id}".encode()).hexdigest()
        phash = hashlib.sha256(payload).hexdigest()
        rec = (event_id, time.time(), op, self.appname, dst_app, user_id, tag_id, phash, json.dumps(meta))
        with sqlite3.connect(self.db_path) as c:
            c.execute("""INSERT INTO provenance(event_id,t_unix,op,src_app,dst_app,user_id,tag_id,payload_hash,meta)
                         VALUES(?,?,?,?,?,?,?,?,?)""", rec)
            c.commit()
        return event_id

# ---------- Shadow runtime (sources/sinks/receive) ----------
POLICY_DEFAULT = {"delete_policy": "delete_all_user_data"}

class ShadowRuntime:
    def __init__(self, appname: str, prov_db: str):
        self.app = appname
        self.log = ProvLogger(db_path=prov_db, appname=appname)

    def source(self, fn: Callable[..., Any]) -> Callable[..., Labeled[Any]]:
        def wrapper(*args, **kwargs):
            u = current_user.get()
            if not u:
                raise RuntimeError("No user context set for source()")
            raw = fn(*args, **kwargs)
            lab = new_label(user_id=u, policies=POLICY_DEFAULT)
            self.log.log("source", u, lab.tag_id, payload=str(raw).encode(), meta={"function": fn.__name__})
            return Labeled(value=raw, label=lab)
        wrapper.__name__ = fn.__name__
        return wrapper

    def sink(self, dst_app: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def deco(fn: Callable[..., Any]):
            def wrapper(labeled: Labeled[Any], *args, **kwargs):
                if not isinstance(labeled, Labeled):
                    raise TypeError("sink() requires a Labeled[...] value")
                self.log.log("transfer_out",
                             labeled.label.user_id, labeled.label.tag_id,
                             payload=str(labeled.value).encode(),
                             dst_app=dst_app,
                             meta={"function": fn.__name__})
                return fn(labeled, *args, **kwargs)
            wrapper.__name__ = fn.__name__
            return wrapper
        return deco

    def receive(self, labeled_header: str, body: Any) -> Labeled[Any]:
        lab = Label.from_header(labeled_header)
        self.log.log("transfer_in", lab.user_id, lab.tag_id, payload=str(body).encode())
        return Labeled(value=body, label=lab)

# ---------- Tiny “unit test” without any web ----------
def print_log(db_path: str, title: str):
    print(f"\n== {title} ({db_path}) ==")
    with sqlite3.connect(db_path) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute("SELECT t_unix, op, src_app, COALESCE(dst_app,'') as dst, user_id, tag_id FROM provenance ORDER BY t_unix").fetchall()
        for r in rows:
            ts = time.strftime("%H:%M:%S", time.localtime(r["t_unix"]))
            print(f"{ts} | {r['op']:<13} | {r['src_app']:<11} -> {r['dst']:<7} | user={r['user_id']} | tag={r['tag_id'][:8]}...")

if __name__ == "__main__":
    # Two runtimes, two separate provenance DBs (simulate two apps)
    pp_db = "./prov_pencilpros.db"
    py_db = "./prov_paypal.db"
    if os.path.exists(pp_db): os.remove(pp_db)
    if os.path.exists(py_db): os.remove(py_db)

    pencilpros = ShadowRuntime("PencilPros", pp_db)
    paypal     = ShadowRuntime("PayPal",     py_db)

    # Define a source on PencilPros
    @pencilpros.source
    def build_payment_blob(user_id: str, amount_cents: int, item: str) -> dict:
        return {"user_id": user_id, "amount_cents": amount_cents, "item": item, "ts": time.time()}

    # Define a sink on PencilPros that "delivers" directly to PayPal.receive (no HTTP)
    @pencilpros.sink(dst_app="PayPal")
    def deliver_to_paypal(labeled: Labeled[dict]):
        header = labeled.label.to_header()
        # PayPal receives the labeled payload
        _ = paypal.receive(header, labeled.value)
        return "ok"

    # Simulate a user action end-to-end
    user = "u123"
    token = current_user.set(user)
    try:
        labeled_payment = build_payment_blob(user, 299, "Pencil HB")
    finally:
        current_user.reset(token)

    deliver_to_paypal(labeled_payment)

    # Optional: simulate deletion cascade
    pencilpros.log.log("delete_request", user, tag_id="*", payload=b"", dst_app="PayPal", meta={"why":"user GDPR request"})
    paypal.log.log("delete_done", user, tag_id="*", payload=b"", meta={"receipt":"ok"})

    # Show logs from each app
    print_log(pp_db, "PencilPros provenance")
    print_log(py_db, "PayPal provenance")

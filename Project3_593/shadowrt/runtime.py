from __future__ import annotations
from typing import Callable, Any
from .labels import current_user, Labeled, Label, new_label
from .provlog import ProvLogger

POLICY_DEFAULT = {"delete_policy": "delete_all_user_data"}

class ShadowRuntime:
    """
    Small runtime that:
      - wraps sources (creates labels + logs 'source')
      - wraps sinks (logs 'transfer_out')
      - can 'receive' labeled data (logs 'transfer_in')
      - exposes prov log for insert/delete/etc.
    """
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
            self.log.log(
                "source", u, lab.tag_id,
                payload=str(raw).encode(),
                meta={"function": fn.__name__},
            )
            return Labeled(value=raw, label=lab)
        wrapper.__name__ = fn.__name__
        return wrapper

    def sink(self, dst_app: str):
        def deco(fn: Callable[..., Any]):
            def wrapper(labeled: Labeled[Any], *args, **kwargs):
                if not isinstance(labeled, Labeled):
                    raise TypeError("sink() requires a Labeled[...] value")
                self.log.log(
                    "transfer_out",
                    labeled.label.user_id,
                    labeled.label.tag_id,
                    payload=str(labeled.value).encode(),
                    dst_app=dst_app,
                    meta={"function": fn.__name__},
                )
                return fn(labeled, *args, **kwargs)
            wrapper.__name__ = fn.__name__
            return wrapper
        return deco

    def receive(self, labeled_header: str, body: Any) -> Labeled[Any]:
        lab = Label.from_header(labeled_header)
        self.log.log(
            "transfer_in",
            lab.user_id,
            lab.tag_id,
            payload=str(body).encode(),
        )
        return Labeled(value=body, label=lab)

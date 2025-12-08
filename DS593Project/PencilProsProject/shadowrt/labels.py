from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, TypeVar
from contextvars import ContextVar
import json, uuid

T = TypeVar("T")

# Who is the current user for this thread/call?
current_user: ContextVar[str | None] = ContextVar("current_user", default=None)

@dataclass(frozen=True)
class Label:
    user_id: str
    tag_id: str
    policies: dict

    def to_header(self) -> str:
        return json.dumps({
            "user_id": self.user_id,
            "tag_id": self.tag_id,
            "policies": self.policies,
        })

    @staticmethod
    def from_header(s: str) -> "Label":
        d = json.loads(s)
        return Label(
            user_id=d["user_id"],
            tag_id=d["tag_id"],
            policies=d["policies"],
        )

def new_label(user_id: str, policies: dict) -> Label:
    return Label(user_id=user_id, tag_id=str(uuid.uuid4()), policies=policies)

@dataclass
class Labeled(Generic[T]):
    value: T
    label: Label

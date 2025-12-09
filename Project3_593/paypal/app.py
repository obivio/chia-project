from fastapi import FastAPI, Header
from pydantic import BaseModel
import json

from shadowrt.runtime import ShadowRuntime
from .db import init_db, insert_payment, delete_payments_by_user

app = FastAPI(title="PayPal-like Service")

init_db()
rt = ShadowRuntime(appname="PayPal", prov_db="paypal_prov.db")

class Charge(BaseModel):
    user_id: str
    amount_cents: int
    item: str
    billing_address: str
    ts: float | None = None  # forwarded from PencilPros

@app.post("/charge")
def charge(charge: Charge, x_shadow_label: str = Header(...)):
    """
    Called by PencilPros sink. We:
      - reconstruct the label (transfer_in provenance)
      - insert into PayPal DB
      - log the DB insertion in PayPal provenance
    """
    labeled = rt.receive(x_shadow_label, charge.model_dump())

    payment_id = insert_payment(
        labeled.value["user_id"],
        labeled.value["billing_address"],
        labeled.value["item"],
        labeled.value["amount_cents"],
    )

    rt.log.log(
        op="insert_payment",
        user_id=labeled.label.user_id,
        tag_id=f"payment:{payment_id}",
        payload=json.dumps(labeled.value).encode(),
        dst_app=None,
        meta={},
    )

    return {"ok": True, "payment_id": payment_id}

@app.delete("/delete_by_user/{user_id}")
def delete_by_user(user_id: str):
    """
    Delete all PayPal records for this user, and log it in provenance.
    """
    deleted = delete_payments_by_user(user_id)

    receipt = {
        "deleted_user_id": user_id,
        "deleted_records": deleted,
    }

    rt.log.log(
        op="delete_done",
        user_id=user_id,
        tag_id="*",
        payload=json.dumps(receipt).encode(),
        dst_app=None,
        meta={},
    )

    return receipt

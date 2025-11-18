from fastapi import FastAPI, Header
from pydantic import BaseModel
from typing import List
from shadowrt.runtime import ShadowRuntime

# http://127.0.0.1:8001/docs

app = FastAPI(title="PayPal-like Service")
rt = ShadowRuntime(appname="PayPal", prov_db="paypal_prov.db")

class Payment(BaseModel):
    user_id: str
    amount_cents: int
    item: str
    ts: float

# super simple storage just for demo
PAYMENTS: List[dict] = []

@app.post("/ingest")
def ingest(payment: Payment, x_shadow_label: str = Header(...)):
    # Rebuild labeled value & log transfer_in
    labeled = rt.receive(x_shadow_label, payment.model_dump())
    PAYMENTS.append(labeled.value)
    return {"ok": True, "stored_count": len(PAYMENTS)}

@app.delete("/delete_by_user/{user_id}")
def delete_by_user(user_id: str):

    global PAYMENTS
    before = len(PAYMENTS)
    PAYMENTS = [p for p in PAYMENTS if p["user_id"] != user_id]
    after = len(PAYMENTS)

    # Log deletion done
    rt.log.log(
        "delete_done",
        user_id,
        "*",
        payload=f"deleted {before-after} records".encode(),
        meta={},
    )
    return {
        "deleted_user_id": user_id,
        "deleted_records": before - after,
    }

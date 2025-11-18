from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time, os, requests
from shadowrt.runtime import ShadowRuntime
from shadowrt.labels import current_user, Labeled

# PencilPros: http://127.0.0.1:8000/docs

app = FastAPI(title="PencilPros Shop")
rt = ShadowRuntime(appname="PencilPros", prov_db="pencilpros_prov.db")

PAYPAL_URL = os.environ.get("PAYPAL_URL", "http://127.0.0.1:8001")

class Purchase(BaseModel):
    user_id: str
    item: str
    amount_cents: int

@rt.source
def build_payment_blob(user_id: str, amount_cents: int, item: str) -> dict:
    return {
        "user_id": user_id,
        "amount_cents": amount_cents,
        "item": item,
        "ts": time.time(),
    }

@rt.sink(dst_app="PayPal")
def send_to_paypal(labeled: Labeled[dict]):
    header = labeled.label.to_header()
    resp = requests.post(
        f"{PAYPAL_URL}/ingest",
        json=labeled.value,
        headers={"X-Shadow-Label": header},
        timeout=5,
    )
    if not resp.ok:
        raise HTTPException(status_code=502, detail="PayPal error")
    return resp.json()

@app.post("/purchase")
def purchase(p: Purchase):
    # Set current_user for the source()
    token = current_user.set(p.user_id)
    try:
        labeled = build_payment_blob(p.user_id, p.amount_cents, p.item)
    finally:
        current_user.reset(token)

    paypal_result = send_to_paypal(labeled)
    return {"ok": True, "paypal_result": paypal_result}

@app.delete("/delete/{user_id}")
def delete_user(user_id: str):
    # Log that we are sending a delete request
    rt.log.log(
        "delete_request",
        user_id,
        "*",
        payload=b"",
        dst_app="PayPal",
        meta={"reason": "user deletion request"},
    )

    resp = requests.delete(f"{PAYPAL_URL}/delete_by_user/{user_id}", timeout=5)
    if not resp.ok:
        raise HTTPException(status_code=502, detail="PayPal delete failed")

    # Log that we received delete confirmation
    rt.log.log(
        "delete_done",
        user_id,
        "*",
        payload=resp.content,
        meta={},
    )

    return {"ok": True, "paypal_receipt": resp.json()}

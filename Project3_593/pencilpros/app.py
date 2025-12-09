from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import time, os, requests, json

from shadowrt.runtime import ShadowRuntime
from shadowrt.labels import current_user, Labeled
from .db import init_db, upsert_user, insert_purchase, delete_user_and_purchases

app = FastAPI(title="PencilPros Shop")

# allow the frontend on port 5500 to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Init DB and runtime
init_db()
rt = ShadowRuntime(appname="PencilPros", prov_db="pencilpros_prov.db")

PAYPAL_URL = os.environ.get("PAYPAL_URL", "http://127.0.0.1:8001")

# ---------- Pydantic models ----------

class UserCreate(BaseModel):
    user_id: str
    name: str

class PurchaseCreate(BaseModel):
    user_id: str
    item: str
    amount_cents: int
    billing_address: str  # sent to PayPal, not stored locally

# ---------- Runtime-wrapped functions ----------

@rt.source
def build_payment_blob(user_id: str, amount_cents: int, item: str) -> dict:
    """
    This is the labeled payload leaving PencilPros.
    Note: no billing address; that's only for PayPal's DB.
    """
    return {
        "user_id": user_id,
        "amount_cents": amount_cents,
        "item": item,
        "ts": time.time(),
    }

@rt.sink(dst_app="PayPal")
def send_to_paypal(labeled: Labeled[dict], billing_address: str):
    """
    Sink: logs transfer_out, then calls PayPal /charge with the label attached.
    """
    header = labeled.label.to_header()
    payload = dict(labeled.value)
    payload["billing_address"] = billing_address

    resp = requests.post(
        f"{PAYPAL_URL}/charge",
        json=payload,
        headers={"X-Shadow-Label": header},
        timeout=5,
    )
    if not resp.ok:
        raise HTTPException(status_code=502, detail="PayPal error")
    return resp.json()

# ---------- API endpoints ----------

@app.post("/user")
def create_or_update_user(u: UserCreate):
    # Update application DB
    upsert_user(u.user_id, u.name)

    # Log into provenance as an insert of user metadata
    rt.log.log(
        op="insert_user",
        user_id=u.user_id,
        tag_id=f"user:{u.user_id}",
        payload=json.dumps({"name": u.name}).encode(),
        dst_app=None,
        meta={},
    )
    return {"ok": True}

@app.post("/purchase")
def purchase(p: PurchaseCreate):
    """
    Creates a purchase locally and sends a labeled payment blob to PayPal.
    """
    # Insert into local DB
    purchase_id = insert_purchase(p.user_id, p.item, p.amount_cents)

    # Log that purchase row exists in our DB
    rt.log.log(
        op="insert_purchase",
        user_id=p.user_id,
        tag_id=f"purchase:{purchase_id}",
        payload=json.dumps({
            "item": p.item,
            "amount_cents": p.amount_cents,
        }).encode(),
        dst_app=None,
        meta={},
    )

    # Build labeled payment blob under current_user
    token = current_user.set(p.user_id)
    try:
        labeled_payment = build_payment_blob(p.user_id, p.amount_cents, p.item)
    finally:
        current_user.reset(token)

    # Send to PayPal with sink (logs transfer_out)
    paypal_result = send_to_paypal(labeled_payment, p.billing_address)

    return {
        "ok": True,
        "purchase_id": purchase_id,
        "paypal_result": paypal_result,
    }

@app.delete("/delete/{user_id}")
def delete_user(user_id: str):
    """
    Deletion request:
      1. Check provenance for destinations of this user's data.
      2. Send delete requests (e.g., to PayPal).
      3. Log delete_request + delete_done in our provenance.
      4. Actually delete from PencilPros DB.
    """
    # 1) Which external apps have we sent this user's data to?
    destinations = rt.log.destinations_for_user(user_id)

    receipts = {}

    # 2) Send deletion requests based on provenance
    if "PayPal" in destinations:
        rt.log.log(
            op="delete_request",
            user_id=user_id,
            tag_id="*",
            payload=b"",
            dst_app="PayPal",
            meta={"reason": "user deletion request"},
        )
        resp = requests.delete(f"{PAYPAL_URL}/delete_by_user/{user_id}", timeout=5)
        if not resp.ok:
            raise HTTPException(status_code=502, detail="PayPal delete failed")

        receipts["PayPal"] = resp.json()

        rt.log.log(
            op="delete_done",
            user_id=user_id,
            tag_id="*",
            payload=json.dumps(receipts["PayPal"]).encode(),
            dst_app="PayPal",
            meta={},
        )

    # 3) Delete from our own DB
    delete_user_and_purchases(user_id)

    # 4) Log local DB deletion (for audit trail)
    rt.log.log(
        op="delete_local",
        user_id=user_id,
        tag_id="*",
        payload=b"",
        dst_app=None,
        meta={"details": "deleted from users + purchases"},
    )

    return {"ok": True, "receipts": receipts}

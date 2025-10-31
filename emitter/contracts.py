# Simple event contract (we'll externalise later)
from pydantic import BaseModel, Field
class TransactionEvent(BaseModel):
    tx_id: str
    customer_id: str
    amount: float = Field(ge=0)
    merchant_cat: str
    ts: int
    label: int | None = None

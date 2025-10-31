# Simple event contract (we'll externalise later)
from pydantic import BaseModel, Field
from typing import Any

class TransactionEvent(BaseModel):
    tx_id: str
    customer_id: str
    amount: float = Field(ge=0)
    merchant_cat: str
    ts: int
    label: int | None = None
    features: dict[str, Any] = Field(default_factory=dict)
    """Additional features from source dataset (e.g., V1-V28 PCA features for ULB dataset)"""

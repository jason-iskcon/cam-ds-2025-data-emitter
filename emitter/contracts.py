# Simple event contract (we'll externalise later)
from pydantic import BaseModel, Field
from typing import Any

class TransactionEvent(BaseModel):
    tx_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    amount: float = Field(ge=0)
    merchant_cat: str = Field(min_length=1)
    ts: int = Field(ge=0)
    label: int | None = Field(default=None, ge=0, le=1)
    features: dict[str, Any] = Field(default_factory=dict)
    """Additional features from source dataset (e.g., V1-V28 PCA features for ULB dataset)"""

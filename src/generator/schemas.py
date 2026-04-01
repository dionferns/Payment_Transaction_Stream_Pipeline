"""
Pydantic models for payment transaction data.

These models serve two purposes:
1. Validate records produced by the generator before they are written.
2. Provide an authoritative field-level specification that the Spark schemas
   in the streaming and batch layers are derived from.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TransactionType(str, Enum):
    PURCHASE = "purchase"
    REFUND = "refund"
    CHARGEBACK = "chargeback"
    P2P = "p2p"


class ResponseCode(str, Enum):
    APPROVED = "00"
    INSUFFICIENT_FUNDS = "51"
    DO_NOT_HONOUR = "05"
    INVALID_MERCHANT = "03"
    EXPIRED_CARD = "54"
    SUSPECTED_FRAUD = "59"
    RESTRICTED_CARD = "62"
    EXCEEDS_WITHDRAWAL_LIMIT = "61"


class PaymentTransaction(BaseModel):
    """A single payment network transaction event."""

    transaction_id: str = Field(..., description="UUID — globally unique per transaction")
    card_hash: str = Field(..., description="SHA-256 hash of PAN — never store raw PAN")
    merchant_id: str = Field(..., description="Acquirer-assigned merchant identifier")
    merchant_category_code: str = Field(..., description="ISO 18245 MCC code (4 digits)")
    amount: Decimal = Field(..., description="Transaction amount in the settlement currency")
    currency: str = Field(..., description="ISO 4217 currency code (e.g. USD, GBP)")
    timestamp: datetime = Field(..., description="UTC event timestamp")
    transaction_type: TransactionType
    country_code: str = Field(..., description="ISO 3166-1 alpha-2 country code")
    acquirer_id: str = Field(..., description="Acquiring bank identifier")
    issuer_id: str = Field(..., description="Card-issuing bank identifier")
    response_code: str = Field(..., description="ISO 8583 response code")
    is_cross_border: bool = Field(
        ..., description="True when merchant country differs from card-issuing country"
    )
    # Optional enrichment fields
    merchant_name: Optional[str] = None
    terminal_id: Optional[str] = None
    auth_code: Optional[str] = None

    @field_validator("merchant_category_code")
    @classmethod
    def validate_mcc(cls, v: str) -> str:
        if not v.isdigit() or len(v) != 4:
            raise ValueError(f"MCC must be a 4-digit string, got: {v!r}")
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        if len(v) != 3 or not v.isalpha():
            raise ValueError(f"Currency must be ISO 4217 (3 alpha chars), got: {v!r}")
        return v.upper()

    @field_validator("country_code")
    @classmethod
    def validate_country(cls, v: str) -> str:
        if len(v) != 2 or not v.isalpha():
            raise ValueError(f"Country code must be ISO 3166-1 alpha-2, got: {v!r}")
        return v.upper()

    @model_validator(mode="after")
    def validate_amount_sign(self) -> "PaymentTransaction":
        """Enforce sign conventions:
        - Purchases and P2P must be positive.
        - Refunds and chargebacks must be negative.
        """
        if self.transaction_type in (TransactionType.PURCHASE, TransactionType.P2P):
            if self.amount <= 0:
                raise ValueError(
                    f"{self.transaction_type} amount must be positive, got {self.amount}"
                )
        elif self.transaction_type in (TransactionType.REFUND, TransactionType.CHARGEBACK):
            if self.amount >= 0:
                raise ValueError(
                    f"{self.transaction_type} amount must be negative, got {self.amount}"
                )
        return self

    def to_dict(self) -> dict:
        """Serialise to a JSON-compatible dict (amounts as strings for precision)."""
        data = self.model_dump()
        data["amount"] = str(self.amount)
        data["timestamp"] = self.timestamp.isoformat()
        data["transaction_type"] = self.transaction_type.value
        return data


class TransactionBatch(BaseModel):
    """A batch of transactions flushed to the landing zone in one write."""

    transactions: list[PaymentTransaction]
    batch_id: str
    generated_at: datetime
    target_date: str   # YYYY-MM-DD
    target_hour: int   # 0-23

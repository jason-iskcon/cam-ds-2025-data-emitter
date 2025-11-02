"""Tests for TransactionEvent validation."""

import pytest
from pydantic import ValidationError

from emitter.contracts import TransactionEvent


class TestTransactionEvent:
    """Test TransactionEvent Pydantic model validation."""

    def test_valid_event_all_fields(self):
        """Test valid event with all fields."""
        event = TransactionEvent(
            tx_id="tx_1",
            customer_id="c123",
            amount=99.99,
            merchant_cat="grocery",
            ts=1000,
            label=0,
            features={"V1": 0.5, "V2": -0.2},
        )
        assert event.tx_id == "tx_1"
        assert event.customer_id == "c123"
        assert event.amount == 99.99
        assert event.merchant_cat == "grocery"
        assert event.ts == 1000
        assert event.label == 0
        assert event.features == {"V1": 0.5, "V2": -0.2}

    def test_valid_event_minimal_fields(self):
        """Test valid event with only required fields."""
        event = TransactionEvent(
            tx_id="tx_1", customer_id="c123", amount=50.0, merchant_cat="electronics", ts=2000
        )
        assert event.tx_id == "tx_1"
        assert event.amount == 50.0
        assert event.label is None  # Default
        assert event.features == {}  # Default empty dict

    def test_invalid_negative_amount(self):
        """Test negative amount raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            TransactionEvent(
                tx_id="tx_1",
                customer_id="c123",
                amount=-10.0,  # Invalid: negative
                merchant_cat="grocery",
                ts=1000,
            )
        assert "ge" in str(exc_info.value).lower() or "greater" in str(exc_info.value).lower()

    def test_zero_amount_valid(self):
        """Test zero amount is valid (ge=0 allows zero)."""
        event = TransactionEvent(
            tx_id="tx_1", customer_id="c123", amount=0.0, merchant_cat="grocery", ts=1000
        )
        assert event.amount == 0.0

    def test_missing_required_field(self):
        """Test missing required field raises ValidationError."""
        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="tx_1",
                customer_id="c123",
                # Missing amount
                merchant_cat="grocery",
                ts=1000,
            )

        with pytest.raises(ValidationError):
            TransactionEvent(
                # Missing tx_id
                customer_id="c123",
                amount=50.0,
                merchant_cat="grocery",
                ts=1000,
            )

    def test_label_optional(self):
        """Test label can be None or int."""
        event1 = TransactionEvent(
            tx_id="tx_1",
            customer_id="c123",
            amount=50.0,
            merchant_cat="grocery",
            ts=1000,
            label=None,
        )
        assert event1.label is None

        event2 = TransactionEvent(
            tx_id="tx_2", customer_id="c123", amount=50.0, merchant_cat="grocery", ts=1000, label=1
        )
        assert event2.label == 1

    def test_features_default_empty_dict(self):
        """Test features defaults to empty dict."""
        event = TransactionEvent(
            tx_id="tx_1", customer_id="c123", amount=50.0, merchant_cat="grocery", ts=1000
        )
        assert event.features == {}
        assert isinstance(event.features, dict)

    def test_features_can_contain_any_values(self):
        """Test features dict can contain various types."""
        event = TransactionEvent(
            tx_id="tx_1",
            customer_id="c123",
            amount=50.0,
            merchant_cat="grocery",
            ts=1000,
            features={
                "V1": 0.5,
                "V2": -0.2,
                "V3": 1.23,
                "Time": 100,
                "name": "test",
                "nested": {"key": "value"},
            },
        )
        assert event.features["V1"] == 0.5
        assert event.features["Time"] == 100
        assert event.features["name"] == "test"
        assert event.features["nested"] == {"key": "value"}

    def test_model_dump(self):
        """Test model_dump() returns dictionary representation."""
        event = TransactionEvent(
            tx_id="tx_1",
            customer_id="c123",
            amount=99.99,
            merchant_cat="grocery",
            ts=1000,
            label=1,
            features={"V1": 0.5},
        )
        dumped = event.model_dump()
        assert isinstance(dumped, dict)
        assert dumped["tx_id"] == "tx_1"
        assert dumped["customer_id"] == "c123"
        assert dumped["amount"] == 99.99
        assert dumped["merchant_cat"] == "grocery"
        assert dumped["ts"] == 1000
        assert dumped["label"] == 1
        assert dumped["features"] == {"V1": 0.5}

    def test_invalid_negative_timestamp(self):
        """Test negative timestamp raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            TransactionEvent(
                tx_id="tx_1",
                customer_id="c123",
                amount=50.0,
                merchant_cat="grocery",
                ts=-1000,  # Invalid: negative
            )
        assert "ge" in str(exc_info.value).lower() or "greater" in str(exc_info.value).lower()

    def test_zero_timestamp_valid(self):
        """Test zero timestamp is valid (ge=0 allows zero)."""
        event = TransactionEvent(
            tx_id="tx_1", customer_id="c123", amount=50.0, merchant_cat="grocery", ts=0
        )
        assert event.ts == 0

    def test_empty_string_fields(self):
        """Test empty strings in required string fields raise ValidationError."""
        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="",  # Invalid: empty
                customer_id="c123",
                amount=50.0,
                merchant_cat="grocery",
                ts=1000,
            )

        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="tx_1",
                customer_id="",  # Invalid: empty
                amount=50.0,
                merchant_cat="grocery",
                ts=1000,
            )

        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="tx_1",
                customer_id="c123",
                amount=50.0,
                merchant_cat="",  # Invalid: empty
                ts=1000,
            )

    def test_label_binary_constraint(self):
        """Test label must be 0 or 1 if provided."""
        # Valid: 0 or 1
        event1 = TransactionEvent(
            tx_id="tx_1", customer_id="c123", amount=50.0, merchant_cat="grocery", ts=1000, label=0
        )
        assert event1.label == 0

        event2 = TransactionEvent(
            tx_id="tx_2", customer_id="c123", amount=50.0, merchant_cat="grocery", ts=1000, label=1
        )
        assert event2.label == 1

        # Invalid: negative
        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="tx_3",
                customer_id="c123",
                amount=50.0,
                merchant_cat="grocery",
                ts=1000,
                label=-1,
            )

        # Invalid: > 1
        with pytest.raises(ValidationError):
            TransactionEvent(
                tx_id="tx_4",
                customer_id="c123",
                amount=50.0,
                merchant_cat="grocery",
                ts=1000,
                label=2,
            )

    def test_type_coercion(self):
        """Test Pydantic's type coercion behavior."""
        # Pydantic will coerce compatible types
        event = TransactionEvent(
            tx_id="tx_1",
            customer_id="c123",
            amount="50.5",  # String to float
            merchant_cat="grocery",
            ts="1000",  # String to int
        )
        assert event.amount == 50.5
        assert isinstance(event.amount, float)
        assert event.ts == 1000
        assert isinstance(event.ts, int)

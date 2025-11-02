"""Tests for StdoutSink."""

import json

import pytest

from emitter.sinks import StdoutSink


class TestStdoutSink:
    """Test StdoutSink functionality."""

    def test_write_basic_event(self, capsys):
        """Test writing a basic event."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 99.99,
            "merchant_cat": "grocery",
            "ts": 1000,
            "label": 0,
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()

        # Should output valid JSON
        assert output
        parsed = json.loads(output)
        assert parsed == event

    def test_write_event_with_features(self, capsys):
        """Test writing event with features dict."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "label": 1,
            "features": {"V1": 0.5, "V2": -0.2, "Time": 100},
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()
        parsed = json.loads(output)
        assert parsed == event
        assert parsed["features"]["V1"] == 0.5
        assert parsed["features"]["Time"] == 100

    def test_write_multiple_events(self, capsys):
        """Test writing multiple events (one per line)."""
        events = [
            {
                "tx_id": "tx_1",
                "customer_id": "c1",
                "amount": 50.0,
                "merchant_cat": "grocery",
                "ts": 1000,
            },
            {
                "tx_id": "tx_2",
                "customer_id": "c2",
                "amount": 75.0,
                "merchant_cat": "electronics",
                "ts": 1001,
            },
            {
                "tx_id": "tx_3",
                "customer_id": "c3",
                "amount": 100.0,
                "merchant_cat": "fuel",
                "ts": 1002,
            },
        ]
        sink = StdoutSink()
        for event in events:
            sink.write(event)

        captured = capsys.readouterr()
        output = captured.out
        lines = output.strip().split("\n")

        # Should have 3 lines (one per event)
        assert len(lines) == 3

        # Each line should be valid JSON
        for i, line in enumerate(lines):
            parsed = json.loads(line)
            assert parsed == events[i]

    def test_ensure_ascii_false_unicode_support(self, capsys):
        """Test ensure_ascii=False preserves unicode characters."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "商店",  # Chinese: "store"
            "ts": 1000,
            "features": {"name": "café", "city": "São Paulo"},
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()

        # Should preserve unicode
        parsed = json.loads(output)
        assert parsed["merchant_cat"] == "商店"
        assert parsed["features"]["name"] == "café"
        assert parsed["features"]["city"] == "São Paulo"

        # Verify unicode characters are in output string
        assert "商店" in output
        assert "café" in output

    def test_write_event_with_none_values(self, capsys):
        """Test writing event with None values."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "label": None,
            "features": {"V1": 0.5, "V2": None},
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()
        parsed = json.loads(output)

        assert parsed["label"] is None
        assert parsed["features"]["V2"] is None

    def test_write_event_with_complex_features(self, capsys):
        """Test writing event with complex nested features."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "features": {
                "V1": 0.5,
                "V2": -0.2,
                "V28": 1.23,
                "Time": 100,
                "metadata": {"source": "ulb", "version": 1},
            },
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()
        parsed = json.loads(output)

        assert parsed["features"]["metadata"]["source"] == "ulb"
        assert parsed["features"]["metadata"]["version"] == 1

    def test_protocol_compliance(self):
        """Test StdoutSink conforms to Sink protocol."""
        from emitter.sinks import Sink

        sink = StdoutSink()
        # Verify protocol compliance
        assert isinstance(sink, Sink)

    def test_write_empty_features_dict(self, capsys):
        """Test writing event with empty features dict."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "features": {},
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out.strip()
        parsed = json.loads(output)
        assert parsed["features"] == {}

    def test_write_newline_handling(self, capsys):
        """Test that each write produces one line (no extra newlines)."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
        }
        sink = StdoutSink()
        sink.write(event)
        captured = capsys.readouterr()
        output = captured.out

        # Should be exactly one line (print adds newline, so output ends with \n)
        lines = output.rstrip("\n").split("\n")
        assert len(lines) == 1

        # Content should be valid JSON
        assert json.loads(lines[0]) == event

    def test_write_non_serializable_raises_error(self):
        """Test writing non-JSON-serializable data raises ValueError."""
        import datetime

        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "timestamp": datetime.datetime.now(),  # Not JSON serializable
        }
        sink = StdoutSink()

        with pytest.raises(ValueError, match="Failed to serialize"):
            sink.write(event)

    def test_write_circular_reference_raises_error(self):
        """Test writing event with circular reference raises ValueError."""
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "data": {},
        }
        event["data"]["self"] = event  # Create circular reference

        sink = StdoutSink()

        with pytest.raises(ValueError, match="Failed to serialize"):
            sink.write(event)

    def test_write_invalid_type_raises_error(self):
        """Test writing event with non-serializable custom object raises error."""

        class CustomObject:
            pass

        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000,
            "obj": CustomObject(),  # Custom object not JSON serializable
        }
        sink = StdoutSink()

        with pytest.raises(ValueError, match="Failed to serialize"):
            sink.write(event)

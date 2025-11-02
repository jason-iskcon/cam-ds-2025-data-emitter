"""Tests for KafkaSink."""
import sys
import pytest
from unittest.mock import MagicMock, patch

from emitter.enums import KafkaAcks
from emitter.sinks import KafkaSink


class TestKafkaSink:
    """Test KafkaSink functionality."""
    
    @pytest.fixture(scope="class")
    def kafka_mock(self):
        """Fixture for mocking confluent_kafka module, shared across class."""
        mock_producer = MagicMock()
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_module = MagicMock()
        mock_module.Producer = mock_producer_class
        with patch.dict('sys.modules', {'confluent_kafka': mock_module}):
            yield mock_producer_class, mock_producer
    
    def _mock_confluent_kafka(self):
        """Create mock confluent_kafka module (kept for backward compatibility)."""
        mock_producer = MagicMock()
        mock_producer_class = MagicMock(return_value=mock_producer)
        mock_module = MagicMock()
        mock_module.Producer = mock_producer_class
        return patch.dict('sys.modules', {'confluent_kafka': mock_module}), mock_producer_class, mock_producer
    
    def test_init_valid_config(self, kafka_mock):
        """Test KafkaSink initialization with valid config."""
        mock_producer_class, mock_producer = kafka_mock
        sink = KafkaSink(
            bootstrap="localhost:9092",
            topic="transactions",
            acks="1",
            linger_ms=10,
            batch_size=32768
        )
        
        assert sink.topic == "transactions"
        assert sink.acks == "1"
        mock_producer_class.assert_called_once()
        call_kwargs = mock_producer_class.call_args[0][0]
        assert call_kwargs["bootstrap.servers"] == "localhost:9092"
        assert call_kwargs["acks"] == "1"
        assert call_kwargs["linger.ms"] == 10
        assert call_kwargs["batch.size"] == 32768
    
    def test_init_default_acks(self, kafka_mock):
        """Test KafkaSink initialization with default acks."""
        mock_producer_class, mock_producer = kafka_mock
        sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
        
        assert sink.acks == "1"  # Default
        call_kwargs = mock_producer_class.call_args[0][0]
        assert call_kwargs["acks"] == "1"
    
    def test_init_all_acks_modes(self, kafka_mock):
        """Test KafkaSink initialization with all valid acks modes."""
        mock_producer_class, mock_producer = kafka_mock
        for acks in ('0', '1', 'all'):
            # Reset mock call count for each iteration
            mock_producer_class.reset_mock()
            sink = KafkaSink(
                bootstrap="localhost:9092",
                topic="transactions",
                acks=acks
            )
            assert sink.acks == acks
            call_kwargs = mock_producer_class.call_args[0][0]
            assert call_kwargs["acks"] == acks
    
    def test_init_with_enum(self, kafka_mock):
        """Test KafkaSink initialization with KafkaAcks enum."""
        mock_producer_class, mock_producer = kafka_mock
        for acks_enum in KafkaAcks:
            # Reset mock call count for each iteration
            mock_producer_class.reset_mock()
            sink = KafkaSink(
                bootstrap="localhost:9092",
                topic="transactions",
                acks=acks_enum
            )
            assert sink.acks == acks_enum.value
            call_kwargs = mock_producer_class.call_args[0][0]
            assert call_kwargs["acks"] == acks_enum.value
    
    def test_init_invalid_acks_raises_error(self):
        """Test KafkaSink initialization with invalid acks raises ValueError."""
        patcher, _, _ = self._mock_confluent_kafka()
        with patcher:
            with pytest.raises(ValueError, match="acks must be one of"):
                KafkaSink(
                    bootstrap="localhost:9092",
                    topic="transactions",
                    acks="invalid"
                )
            
            with pytest.raises(ValueError, match="acks must be one of"):
                KafkaSink(
                    bootstrap="localhost:9092",
                    topic="transactions",
                    acks="2"
                )
    
    def test_init_missing_confluent_kafka_raises_error(self):
        """Test KafkaSink initialization without confluent-kafka raises ImportError."""
        with patch.dict('sys.modules', {'confluent_kafka': None}):
            with pytest.raises(ImportError, match="confluent-kafka package is required"):
                KafkaSink(bootstrap="localhost:9092", topic="transactions")
    
    def test_context_manager(self, kafka_mock):
        """Test KafkaSink as context manager."""
        mock_producer_class, mock_producer = kafka_mock
        mock_producer.flush.return_value = 0
        with KafkaSink(bootstrap="localhost:9092", topic="transactions") as sink:
            assert sink.acks == "1"
            assert sink.producer == mock_producer
        
        # close() should be called on exit
        mock_producer.flush.assert_called_once()
        assert sink.producer is None
    
    def test_write_calls_producer(self, kafka_mock):
        """Test write method calls producer.produce()."""
        mock_producer_class, mock_producer = kafka_mock
        event = {
            "tx_id": "tx_1",
            "customer_id": "c123",
            "amount": 100.0,
            "merchant_cat": "grocery",
            "ts": 1000
        }
        sink = KafkaSink(bootstrap="localhost:9092", topic="transactions", acks="1")
        sink.write(event)
        
        # Should call produce with topic and serialized JSON
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args
        assert call_args[0][0] == "transactions"  # topic
        # Second arg should be JSON-encoded bytes
        import json
        decoded = json.loads(call_args[0][1].decode('utf-8'))
        assert decoded == event
        
        # Should poll after produce
        mock_producer.poll.assert_called_once_with(0)
    
    def test_write_no_callback_for_acks_zero(self):
        """Test write doesn't use callback when acks='0'."""
        patcher, mock_producer_class, mock_producer = self._mock_confluent_kafka()
        event = {"tx_id": "tx_1", "customer_id": "c1", "amount": 100.0, "merchant_cat": "grocery", "ts": 1000}
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions", acks="0")
            sink.write(event)
            
            # Callback should be None for acks='0'
            call_args = mock_producer.produce.call_args
            assert call_args[1]["callback"] is None
    
    def test_flush_calls_producer_flush(self):
        """Test flush method calls producer.flush()."""
        patcher, mock_producer_class, mock_producer = self._mock_confluent_kafka()
        mock_producer.flush.return_value = 0
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            sink.flush()
            
            mock_producer.flush.assert_called_once_with(timeout=10.0)
    
    @patch('builtins.print')
    def test_flush_warns_on_undelivered(self, mock_print):
        """Test flush warns when messages remain undelivered."""
        patcher, mock_producer_class, mock_producer = self._mock_confluent_kafka()
        mock_producer.flush.return_value = 5  # 5 messages undelivered
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            sink.flush()
            
            # Check print was called with warning message
            mock_print.assert_called()
            call_args = str(mock_print.call_args_list)
            assert "5" in call_args
            assert "undelivered" in call_args.lower()
    
    def test_close_flushes_and_clears_producer(self):
        """Test close method flushes and clears producer."""
        patcher, mock_producer_class, mock_producer = self._mock_confluent_kafka()
        mock_producer.flush.return_value = 0
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            assert sink.producer == mock_producer
            
            sink.close()
            
            mock_producer.flush.assert_called_once()
            assert sink.producer is None
    
    @patch('builtins.print')
    def test_close_handles_flush_error(self, mock_print):
        """Test close handles errors during flush gracefully."""
        patcher, mock_producer_class, mock_producer = self._mock_confluent_kafka()
        mock_producer.flush.side_effect = Exception("Flush error")
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            
            # Should not raise, just warn
            sink.close()
            
            # Should still clear producer
            assert sink.producer is None
            # Should print warning
            mock_print.assert_called()
            call_args = str(mock_print.call_args_list)
            assert "Warning" in call_args or "Error" in call_args
    
    def test_write_non_serializable_raises_error(self):
        """Test writing non-JSON-serializable data raises ValueError."""
        import datetime
        patcher, _, mock_producer = self._mock_confluent_kafka()
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            event = {"ts": datetime.datetime.now()}
            
            with pytest.raises(ValueError, match="Failed to serialize"):
                sink.write(event)
    
    @patch('builtins.print')
    def test_delivery_callback_on_error(self, mock_print):
        """Test delivery callback logs errors."""
        patcher, _, mock_producer = self._mock_confluent_kafka()
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            
            # Simulate delivery error
            error = Exception("Broker unavailable")
            sink._delivery_callback(error, None)
            
            # Should print error
            mock_print.assert_called_once()
            assert "Delivery failed" in str(mock_print.call_args)
    
    def test_operations_after_close_are_safe(self):
        """Test calling methods after close doesn't crash."""
        patcher, _, mock_producer = self._mock_confluent_kafka()
        mock_producer.flush.return_value = 0
        with patcher:
            sink = KafkaSink(bootstrap="localhost:9092", topic="transactions")
            sink.close()
            
            # Should be safe to call again
            sink.close()  # No crash
            sink.flush()  # Should be no-op (producer is None)
            
            # write should also be safe (returns early if producer is None)
            event = {"tx_id": "tx_1", "customer_id": "c1", "amount": 100.0, "merchant_cat": "grocery", "ts": 1000}
            sink.write(event)  # Should return without error


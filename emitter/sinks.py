"""Event sinks for emitting transaction events to different destinations."""
import json
import sys
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Sink(Protocol):
    """Protocol for event sinks that can write transaction events.
    
    Any class implementing a `write(event: dict[str, Any]) -> None` method
    can be used as a sink for streaming events.
    """
    
    def write(self, event: dict[str, Any]) -> None:
        """Write an event to the sink.
        
        Args:
            event: Dictionary representation of a TransactionEvent
        """
        ...


class StdoutSink:
    """Sink that writes events as JSON to stdout."""
    
    def __repr__(self) -> str:
        """Return string representation."""
        return "StdoutSink()"
    
    def write(self, event: dict[str, Any]) -> None:
        """Write event as JSON line to stdout.
        
        Args:
            event: Dictionary representation of a TransactionEvent
            
        Raises:
            ValueError: If event cannot be serialized to JSON
        """
        try:
            print(json.dumps(event, ensure_ascii=False))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize event to JSON: {e}") from e


class KafkaSink:
    """Sink that writes events to a Kafka topic.
    
    Requires the 'confluent-kafka' package to be installed.
    """
    
    def __init__(
        self,
        bootstrap: str,
        topic: str,
        acks: str | None = "1",
        linger_ms: int = 5,
        batch_size: int = 16384,
    ):
        """Initialize Kafka sink.
        
        Args:
            bootstrap: Kafka broker address (e.g., 'localhost:9092' or 'redpanda:9092')
            topic: Kafka topic name to write events to
            acks: Number of acknowledgments required ('0', '1', 'all', or None)
            linger_ms: Milliseconds to wait before sending a batch
            batch_size: Batch size in bytes
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka package is required for KafkaSink. "
                "Install with: pip install confluent-kafka"
            )
        
        self.topic = topic
        valid_acks = ('0', '1', 'all')
        if acks is not None and acks not in valid_acks:
            raise ValueError(f"acks must be one of {valid_acks}, got {acks!r}")
        self.acks = acks or "1"
        
        config = {
            "bootstrap.servers": bootstrap,
            "client.id": "emitter",
            "acks": self.acks,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
        }
        
        self.producer = Producer(config)
    
    def __repr__(self) -> str:
        """Return string representation."""
        return f"KafkaSink(topic={self.topic!r}, acks={self.acks!r})"
    
    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback for message delivery confirmation."""
        if err:
            print(f"Delivery failed: {err}", file=sys.stderr)
    
    def flush(self) -> None:
        """Flush any pending messages to Kafka."""
        if self.producer:
            remaining = self.producer.flush(timeout=10.0)
            if remaining > 0:
                print(f"Warning: {remaining} message(s) remain undelivered after flush", file=sys.stderr)
    
    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self.producer:
            # Flush before closing to ensure all messages are sent
            try:
                self.flush()
            except Exception as e:
                print(f"Warning: Error during final flush: {e}", file=sys.stderr)
            # confluent-kafka Producer doesn't need explicit close, but we'll clear references
            self.producer = None
    
    def write(self, event: dict[str, Any]) -> None:
        """Write event to Kafka topic.
        
        Args:
            event: Dictionary representation of a TransactionEvent
            
        Raises:
            ValueError: If event cannot be serialized to JSON
            RuntimeError: If message production fails
        """
        if not self.producer:
            return  # Already closed, no-op
        try:
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")
            self.producer.produce(
                self.topic,
                value,
                callback=self._delivery_callback if self.acks != '0' else None
            )
            # Poll to trigger delivery callbacks (non-blocking)
            self.producer.poll(0)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize event to JSON: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to produce message to Kafka: {e}") from e
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures close is called."""
        self.close()
        return False


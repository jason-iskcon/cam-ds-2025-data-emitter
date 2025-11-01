"""Event sinks for emitting transaction events to different destinations."""
import json
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
    
    def write(self, event: dict[str, Any]) -> None:
        """Write event as JSON line to stdout.
        
        Args:
            event: Dictionary representation of a TransactionEvent
        """
        print(json.dumps(event, ensure_ascii=False))


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
        self.acks = acks or "1"
        
        config = {
            "bootstrap.servers": bootstrap,
            "client.id": "emitter",
            "acks": self.acks if self.acks in ('0', '1', 'all') else '1',
            "linger.ms": linger_ms,
            "batch.size": batch_size,
        }
        
        self.producer = Producer(config)
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            print(f"Delivery failed: {err}", file=__import__('sys').stderr)
    
    def write(self, event: dict[str, Any]) -> None:
        """Write event to Kafka topic.
        
        Args:
            event: Dictionary representation of a TransactionEvent
        """
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")
        self.producer.produce(
            self.topic,
            value,
            callback=self._delivery_callback if self.acks != '0' else None
        )
        # Poll to trigger delivery callbacks (non-blocking)
        self.producer.poll(0)
    
    def flush(self) -> None:
        """Flush any pending messages to Kafka."""
        if self.producer:
            remaining = self.producer.flush(timeout=10.0)
            if remaining > 0:
                print(f"Warning: {remaining} message(s) remain undelivered after flush", file=__import__('sys').stderr)
    
    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self.producer:
            # Flush before closing to ensure all messages are sent
            try:
                self.flush()
            except Exception as e:
                print(f"Warning: Error during final flush: {e}", file=__import__('sys').stderr)
            # confluent-kafka Producer doesn't need explicit close, but we'll clear references
            self.producer = None


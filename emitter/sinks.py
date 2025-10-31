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
    
    Requires the 'kafka-python' package to be installed.
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
            bootstrap: Kafka broker address (e.g., 'localhost:9092')
            topic: Kafka topic name to write events to
            acks: Number of acknowledgments required ('0', '1', 'all', or None)
            linger_ms: Milliseconds to wait before sending a batch
            batch_size: Batch size in bytes
        """
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise ImportError(
                "kafka-python package is required for KafkaSink. "
                "Install with: pip install kafka-python"
            )
        
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks=acks,
            linger_ms=linger_ms,
            batch_size=batch_size,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
    
    def write(self, event: dict[str, Any]) -> None:
        """Write event to Kafka topic.
        
        Args:
            event: Dictionary representation of a TransactionEvent
        """
        self.producer.send(self.topic, event)
    
    def flush(self) -> None:
        """Flush any pending messages to Kafka."""
        if self.producer:
            self.producer.flush()
    
    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self.producer:
            self.producer.close()


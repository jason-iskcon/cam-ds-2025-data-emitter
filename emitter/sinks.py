"""Event sinks for emitting transaction events to different destinations."""
import json
import sys
from typing import Any, Protocol, runtime_checkable

from emitter.config import KafkaConfig
from emitter.enums import KafkaAcks


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
        bootstrap: str | None = None,
        topic: str | None = None,
        acks: str | KafkaAcks | None = "1",
        linger_ms: int = 5,
        batch_size: int = 16384,
        config: KafkaConfig | None = None,
    ):
        """Initialize Kafka sink.
        
        Args:
            bootstrap: Kafka broker address (e.g., 'localhost:9092' or 'redpanda:9092').
                      Ignored if config is provided.
            topic: Kafka topic name to write events to. Ignored if config is provided.
            acks: Number of acknowledgments required. Can be '0', '1', 'all' (string),
                  KafkaAcks enum, or None (defaults to '1'). Ignored if config is provided.
            linger_ms: Milliseconds to wait before sending a batch. Ignored if config is provided.
            batch_size: Batch size in bytes. Ignored if config is provided.
            config: KafkaConfig object. If provided, overrides individual parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        # Use config if provided, otherwise use individual parameters (backward compatibility)
        if config is None:
            if bootstrap is None or topic is None:
                raise ValueError("bootstrap and topic are required when config is not provided")
            config = KafkaConfig(
                bootstrap=bootstrap,
                topic=topic,
                acks=acks,
                linger_ms=linger_ms,
                batch_size=batch_size,
            )
        
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka package is required for KafkaSink. "
                "Install with: pip install confluent-kafka"
            )
        
        self.topic = config.topic
        self.acks = self._normalize_acks(config.acks)
        
        producer_config = {
            "bootstrap.servers": config.bootstrap,
            "client.id": "emitter",
            "acks": self.acks,
            "linger.ms": config.linger_ms,
            "batch.size": config.batch_size,
        }
        
        self.producer = Producer(producer_config)
    
    @staticmethod
    def _normalize_acks(acks_input: str | KafkaAcks | None) -> str:
        """Normalize acks parameter to string value.
        
        Args:
            acks_input: acks value as string, KafkaAcks enum, or None
            
        Returns:
            Normalized acks string value
            
        Raises:
            ValueError: If string value is not valid
            TypeError: If type is not str, KafkaAcks, or None
        """
        if acks_input is None:
            return "1"
        elif isinstance(acks_input, KafkaAcks):
            return acks_input.value
        elif isinstance(acks_input, str):
            # Validate string value
            valid_strings = {e.value for e in KafkaAcks}
            if acks_input not in valid_strings:
                raise ValueError(
                    f"acks must be one of {valid_strings} or a KafkaAcks enum, got {acks_input!r}"
                )
            return acks_input
        else:
            raise TypeError(
                f"acks must be str, KafkaAcks, or None, got {type(acks_input).__name__}"
            )
    
    def __repr__(self) -> str:
        """Return string representation."""
        # Show enum name if value matches an enum value, otherwise show the value
        try:
            acks_enum = KafkaAcks(self.acks)
            acks_repr = f"KafkaAcks.{acks_enum.name}"
        except ValueError:
            acks_repr = repr(self.acks)
        return f"KafkaSink(topic={self.topic!r}, acks={acks_repr})"
    
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


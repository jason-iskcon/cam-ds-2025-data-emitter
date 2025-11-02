"""Configuration classes for emitter modules."""
from dataclasses import dataclass


@dataclass
class BurstConfig:
    """Configuration for burst behavior in event streaming.
    
    Attributes:
        probability: Probability of entering a burst per event (0.0-1.0)
        multiplier: Rate multiplier during bursts (e.g., 5.0 = 5x faster)
        duration_events: Number of events to emit during a burst
    """
    probability: float = 0.0
    multiplier: float = 5.0
    duration_events: int = 10


@dataclass
class KafkaConfig:
    """Configuration for Kafka sink.
    
    Attributes:
        bootstrap: Kafka broker address (e.g., 'localhost:9092' or 'redpanda:9092')
        topic: Kafka topic name to write events to
        acks: Number of acknowledgments required ('0', '1', 'all'). Can be string or KafkaAcks enum.
        linger_ms: Milliseconds to wait before sending a batch
        batch_size: Batch size in bytes
    """
    bootstrap: str
    topic: str
    acks: str | None = "1"
    linger_ms: int = 5
    batch_size: int = 16384


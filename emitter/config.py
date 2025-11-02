"""Configuration classes for emitter modules."""

from dataclasses import dataclass, field


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


@dataclass
class FraudConfig:
    """Configuration for synthetic fraud detection logic.

    Attributes:
        mcc: List of merchant category codes
        mcc_weights: Weights for selecting merchant categories
        high_value_categories: Set of categories considered high-value for fraud
        threshold_amount: Minimum amount to consider for fraud (exclusive)
        probability: Probability threshold for fraud (0.0-1.0)
        night_hours: Set of hours considered "night hours" (0-5, 23)
    """

    mcc: list[str] = field(
        default_factory=lambda: ["grocery", "electronics", "fuel", "luxury", "online"]
    )
    mcc_weights: list[int] = field(default_factory=lambda: [35, 20, 15, 5, 25])
    high_value_categories: set[str] = field(default_factory=lambda: {"luxury", "online"})
    threshold_amount: int = 300
    probability: float = 0.4
    night_hours: set[int] = field(default_factory=lambda: {*range(6), *range(23, 24)})

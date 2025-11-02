"""Enumerations for type-safe constants."""
from enum import Enum


class KafkaAcks(str, Enum):
    """Kafka acknowledgment modes.
    
    Values:
        ZERO: Fire-and-forget (no acknowledgment required)
        ONE: Wait for leader acknowledgment
        ALL: Wait for all in-sync replicas acknowledgment
    """
    ZERO = '0'
    ONE = '1'
    ALL = 'all'
    
    def __str__(self) -> str:
        """Return string value for Kafka config."""
        return self.value


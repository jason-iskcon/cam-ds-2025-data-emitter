"""Shared utilities for event streaming."""
import json
import time
from typing import Callable
from emitter.contracts import TransactionEvent


def emit_event_json(event: TransactionEvent) -> None:
    """Emit a transaction event as JSON to stdout."""
    print(json.dumps(event.model_dump(), ensure_ascii=False))


def stream_events(
    event_generator: Callable[[int, int], TransactionEvent],
    rate_per_sec: float,
    max_events: int | None = None,
) -> None:
    """Stream events to stdout at specified rate.
    
    Args:
        event_generator: Function that generates events (event_num, base_ts) -> TransactionEvent
        rate_per_sec: Events per second to emit
        max_events: Maximum number of events (None for unlimited)
    """
    interval = 1.0 / rate_per_sec
    base_ts = int(time.time())
    event_num = 0
    
    while max_events is None or event_num < max_events:
        event = event_generator(event_num, base_ts)
        emit_event_json(event)
        event_num += 1
        
        if max_events is None or event_num < max_events:
            time.sleep(interval)

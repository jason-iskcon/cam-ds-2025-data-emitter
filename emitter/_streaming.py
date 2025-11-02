"""Shared utilities for event streaming."""
import argparse
import random
import signal
import time
from typing import Callable

from emitter.config import BurstConfig
from emitter.contracts import TransactionEvent
from emitter.sinks import Sink, StdoutSink

# Global flag for graceful shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global _shutdown_requested
    _shutdown_requested = True


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

# Default burst configuration constants
DEFAULT_BURST_MULTIPLIER = 5.0
DEFAULT_BURST_DURATION = 10


def add_streaming_args(parser: argparse.ArgumentParser) -> None:
    """Add common streaming arguments (jitter, burst) to argument parser."""
    parser.add_argument("--jitter", type=float, default=0.0, help="Jitter fraction (0.0-1.0) for timing variance")
    parser.add_argument("--burst-prob", type=float, default=0.0, help="Probability of burst per event (0.0-1.0)")
    parser.add_argument("--burst-mult", type=float, default=DEFAULT_BURST_MULTIPLIER, help="Rate multiplier during bursts")
    parser.add_argument("--burst-duration", type=int, default=DEFAULT_BURST_DURATION, help="Number of events in a burst")


def emit_event(event: TransactionEvent, sink: Sink) -> None:
    """Emit a transaction event to the specified sink.
    
    Args:
        event: TransactionEvent to emit
        sink: Sink to write the event to
    """
    sink.write(event.model_dump())


def sleep_with_jitter(seconds: float, jitter: float = 0.0) -> bool:
    """Sleep ~seconds with optional jitter fraction (0.0–1.0).
    
    Args:
        seconds: Base sleep duration
        jitter: Jitter fraction (0.0-1.0). jitter=0.2 → N(seconds, (0.2*seconds)^2) clamped to >= 0
    
    Returns:
        True if sleep completed, False if interrupted by shutdown signal
    """
    global _shutdown_requested
    if seconds <= 0:
        return not _shutdown_requested
    
    if jitter <= 0:
        duration = seconds
    else:
        std = jitter * seconds
        duration = max(0.0, random.gauss(seconds, std))
    
    # Sleep in small increments to allow checking shutdown flag
    elapsed = 0.0
    chunk = min(0.1, duration)  # Check every 100ms
    while elapsed < duration and not _shutdown_requested:
        remaining = duration - elapsed
        sleep_time = min(chunk, remaining)
        time.sleep(sleep_time)
        elapsed += sleep_time
    
    return not _shutdown_requested


class BurstController:
    """Manages burst state and timing for event streaming."""
    
    def __init__(
        self,
        probability: float,
        multiplier: float,
        duration_events: int,
        base_rate: float,
    ):
        """Initialize burst controller.
        
        Args:
            probability: Probability of entering a burst per event (0.0-1.0)
            multiplier: Rate multiplier during bursts
            duration_events: Number of events to emit during a burst
            base_rate: Base events per second rate
            
        Raises:
            AssertionError: If parameters are invalid
        """
        assert 0.0 <= probability <= 1.0, "probability must be in [0, 1]"
        assert multiplier > 0, "multiplier must be positive"
        assert duration_events > 0, "duration_events must be positive"
        assert base_rate > 0, "base_rate must be positive"
        
        self.probability = probability
        self.multiplier = multiplier
        self.duration_events = duration_events
        self.base_rate = base_rate
        self.remaining = 0
        self.burst_rate = base_rate * multiplier
    
    def should_start_burst(self) -> bool:
        """Check if a new burst should start."""
        return self.probability > 0 and self.remaining == 0 and random.random() < self.probability
    
    def start_burst(self) -> None:
        """Start a new burst (only if not already in one)."""
        if self.remaining == 0:
            self.remaining = self.duration_events
    
    def tick(self) -> float:
        """Advance burst state and return current interval.
        
        Returns:
            Sleep interval in seconds for current state
        """
        if self.remaining > 0:
            self.remaining -= 1
            return 1.0 / self.burst_rate
        return 1.0 / self.base_rate


def _should_continue(event_num: int, max_events: int | None) -> bool:
    """Check if event streaming should continue.
    
    Args:
        event_num: Current event number
        max_events: Maximum number of events (None for unlimited)
        
    Returns:
        True if should continue, False if should stop
    """
    return not _shutdown_requested and (max_events is None or event_num < max_events)


def _handle_burst_and_sleep(
    burst_controller: BurstController,
    jitter: float,
) -> bool:
    """Handle burst logic and sleep with jitter.
    
    Args:
        burst_controller: Burst controller instance
        jitter: Jitter fraction (0.0-1.0) for timing variance
        
    Returns:
        True if sleep completed, False if shutdown requested
    """
    if burst_controller.should_start_burst():
        burst_controller.start_burst()
    
    interval = burst_controller.tick()
    return sleep_with_jitter(interval, jitter)


def _cleanup_sink(sink: Sink) -> None:
    """Ensure sink is properly flushed and closed.
    
    Args:
        sink: Sink instance to cleanup
    """
    if hasattr(sink, 'flush'):
        sink.flush()
    if hasattr(sink, 'close'):
        sink.close()


def stream_events(
    event_generator: Callable[[int, int], TransactionEvent],
    rate_per_sec: float,
    sink: Sink | None = None,
    max_events: int | None = None,
    jitter: float = 0.0,
    burst_probability: float = 0.0,
    burst_multiplier: float = DEFAULT_BURST_MULTIPLIER,
    burst_duration_events: int = DEFAULT_BURST_DURATION,
    burst_config: BurstConfig | None = None,
) -> None:
    """Stream events to a sink at specified rate with optional jitter and bursts.
    
    Args:
        event_generator: Function that generates events (event_num, base_ts) -> TransactionEvent
        rate_per_sec: Base events per second to emit
        sink: Sink to write events to (defaults to StdoutSink if None)
        max_events: Maximum number of events (None for unlimited)
        jitter: Jitter fraction (0.0-1.0) for timing variance. 0.2 = ±20% timing variation
        burst_probability: Probability of entering a burst (0.0-1.0). 0.05 = 5% chance per event.
                          Ignored if burst_config is provided.
        burst_multiplier: Rate multiplier during bursts (e.g., 5.0 = 5x faster).
                         Ignored if burst_config is provided.
        burst_duration_events: Number of events to emit during a burst.
                               Ignored if burst_config is provided.
        burst_config: BurstConfig object. If provided, overrides individual burst parameters.
    """
    if sink is None:
        sink = StdoutSink()
    
    setup_signal_handlers()
    
    # Use burst_config if provided, otherwise use individual parameters (backward compatibility)
    if burst_config is None:
        burst_config = BurstConfig(
            probability=burst_probability,
            multiplier=burst_multiplier,
            duration_events=burst_duration_events,
        )
    
    base_ts = int(time.time())
    event_num = 0
    burst_controller = BurstController(
        probability=burst_config.probability,
        multiplier=burst_config.multiplier,
        duration_events=burst_config.duration_events,
        base_rate=rate_per_sec,
    )
    
    try:
        while _should_continue(event_num, max_events):
            event = event_generator(event_num, base_ts)
            emit_event(event, sink)
            event_num += 1
            
            if _should_continue(event_num, max_events):
                if not _handle_burst_and_sleep(burst_controller, jitter):
                    break  # Shutdown requested during sleep
    finally:
        _cleanup_sink(sink)

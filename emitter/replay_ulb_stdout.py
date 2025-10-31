import argparse

import pandas as pd

from emitter._streaming import DEFAULT_BURST_DURATION, DEFAULT_BURST_MULTIPLIER, add_streaming_args, stream_events
from emitter.contracts import TransactionEvent
from emitter.sinks import KafkaSink, Sink, StdoutSink

DEFAULT_CSV_PATH = "data/ulb/creditcard.csv"
DEFAULT_RATE = 10.0
CUSTOMER_ID_MODULO = 5000


def _get_column_value(row: pd.Series, primary: str, fallback: str = "", default=None):
    """Get column value with fallback column name."""
    if primary in row:
        return row[primary]
    if fallback and fallback in row:
        return row[fallback]
    return default


class ULBEventGenerator:
    """Generator for ULB credit card dataset events."""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df: pd.DataFrame | None = None
        self._load_data()
    
    def _load_data(self) -> None:
        """Load CSV data."""
        self.df = pd.read_csv(self.csv_path)
    
    def __call__(self, event_num: int, base_ts: int) -> TransactionEvent:
        """Generate event from CSV row at position event_num % len(df)."""
        if self.df is None or len(self.df) == 0:
            raise ValueError("No data available")
        
        row_idx = event_num % len(self.df)
        row = self.df.iloc[row_idx]
        
        customer_id = _get_column_value(row, "CustomerID", "") or f"c{event_num % CUSTOMER_ID_MODULO}"
        amount = float(_get_column_value(row, "Amount", "amount", 0.0))
        label = int(_get_column_value(row, "Class", "label", 0))
        
        return TransactionEvent(
            tx_id=f"ulb_{event_num}",
            customer_id=customer_id,
            amount=amount,
            merchant_cat="unknown",
            ts=base_ts + event_num,
            label=label,
        )


def create_sink(args: argparse.Namespace) -> Sink:
    """Create appropriate sink based on command-line arguments.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Sink instance (StdoutSink or KafkaSink)
    """
    if args.kafka_bootstrap:
        return KafkaSink(
            bootstrap=args.kafka_bootstrap,
            topic=args.kafka_topic,
            acks=args.kafka_acks,
            linger_ms=args.kafka_linger_ms,
            batch_size=args.kafka_batch_size,
        )
    return StdoutSink()


def replay(
    csv_path: str,
    rate: float = DEFAULT_RATE,
    loop: bool = False,
    sink: Sink | None = None,
    jitter: float = 0.0,
    burst_probability: float = 0.0,
    burst_multiplier: float = DEFAULT_BURST_MULTIPLIER,
    burst_duration_events: int = DEFAULT_BURST_DURATION,
):
    """Replay ULB credit card dataset events to a sink at specified rate.
    
    Args:
        csv_path: Path to CSV file to replay
        rate: Events per second
        loop: If True, loop indefinitely; otherwise replay once
        sink: Sink to write events to (defaults to StdoutSink if None)
        jitter: Jitter fraction (0.0-1.0) for timing variance
        burst_probability: Probability of burst per event (0.0-1.0)
        burst_multiplier: Rate multiplier during bursts
        burst_duration_events: Number of events in a burst
    """
    generator = ULBEventGenerator(csv_path)
    max_events = None if loop else (len(generator.df) if generator.df is not None else None)
    stream_events(
        generator,
        rate_per_sec=rate,
        sink=sink,
        max_events=max_events,
        jitter=jitter,
        burst_probability=burst_probability,
        burst_multiplier=burst_multiplier,
        burst_duration_events=burst_duration_events,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=DEFAULT_CSV_PATH, help="Path to CSV file")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Events per second")
    parser.add_argument("--loop", action="store_true", help="Loop replay indefinitely")
    add_streaming_args(parser)
    
    # Sink selection arguments
    parser.add_argument("--kafka-bootstrap", type=str, help="Kafka broker address (e.g., localhost:9092). If not provided, uses stdout")
    parser.add_argument("--kafka-topic", type=str, default="transactions", help="Kafka topic name (default: transactions)")
    parser.add_argument("--kafka-acks", type=str, default="1", help="Kafka acks setting: '0', '1', 'all' (default: '1')")
    parser.add_argument("--kafka-linger-ms", type=int, default=5, help="Kafka linger_ms (default: 5)")
    parser.add_argument("--kafka-batch-size", type=int, default=16384, help="Kafka batch_size in bytes (default: 16384)")
    
    args = parser.parse_args()
    sink = create_sink(args)
    
    replay(
        args.path,
        rate=args.rate,
        loop=args.loop,
        sink=sink,
        jitter=args.jitter,
        burst_probability=args.burst_prob,
        burst_multiplier=args.burst_mult,
        burst_duration_events=args.burst_duration,
    )

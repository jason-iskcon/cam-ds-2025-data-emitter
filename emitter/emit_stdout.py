import argparse
import random

from emitter._streaming import add_streaming_args, stream_events
from emitter.contracts import TransactionEvent
from emitter.sinks import KafkaSink, Sink, StdoutSink

MCC = ["grocery", "electronics", "fuel", "luxury", "online"]
MCC_WEIGHTS = [35, 20, 15, 5, 25]
NIGHT_HOURS = {*range(6), *range(23, 24)}
HIGH_VALUE_CATEGORIES = {"luxury", "online"}
LABEL_THRESHOLD_AMOUNT = 300
LABEL_PROBABILITY = 0.4
DEFAULT_RATE = 5.0
DEFAULT_MAX_EVENTS = 10


def synth_event(event_num: int, base_ts: int) -> TransactionEvent:
    """Generate a synthetic transaction event."""
    customer_id = f"c{random.randint(1, 5000)}"
    merchant_cat = random.choices(MCC, weights=MCC_WEIGHTS)[0]
    amount = round(random.lognormvariate(3.2, 0.9), 2)
    
    event_ts = base_ts + event_num
    hour = (event_ts // 3600) % 24
    is_night = hour in NIGHT_HOURS
    
    is_suspicious = (
        amount > LABEL_THRESHOLD_AMOUNT
        and merchant_cat in HIGH_VALUE_CATEGORIES
        and is_night
        and random.random() < LABEL_PROBABILITY
    )
    
    return TransactionEvent(
        tx_id=f"tx_{event_num}",
        customer_id=customer_id,
        amount=amount,
        merchant_cat=merchant_cat,
        ts=event_ts,
        label=int(is_suspicious),
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Events per second")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX_EVENTS, help="Stop after N events")
    add_streaming_args(parser)
    
    # Sink selection arguments
    parser.add_argument("--kafka-bootstrap", type=str, help="Kafka broker address (e.g., localhost:9092). If not provided, uses stdout")
    parser.add_argument("--kafka-topic", type=str, default="transactions", help="Kafka topic name (default: transactions)")
    parser.add_argument("--kafka-acks", type=str, default="1", help="Kafka acks setting: '0', '1', 'all' (default: '1')")
    parser.add_argument("--kafka-linger-ms", type=int, default=5, help="Kafka linger_ms (default: 5)")
    parser.add_argument("--kafka-batch-size", type=int, default=16384, help="Kafka batch_size in bytes (default: 16384)")
    
    args = parser.parse_args()
    sink = create_sink(args)
    
    stream_events(
        synth_event,
        rate_per_sec=args.rate,
        sink=sink,
        max_events=args.max,
        jitter=args.jitter,
        burst_probability=args.burst_prob,
        burst_multiplier=args.burst_mult,
        burst_duration_events=args.burst_duration,
    )

import argparse
import random

from emitter._streaming import add_streaming_args, stream_events
from emitter.common import add_kafka_args, create_sink
from emitter.config import FraudConfig
from emitter.contracts import TransactionEvent

DEFAULT_RATE = 5.0
DEFAULT_MAX_EVENTS = 10
DEFAULT_FRAUD_CONFIG = FraudConfig()


def synth_event(
    event_num: int,
    base_ts: int,
    fraud_config: FraudConfig = DEFAULT_FRAUD_CONFIG,
) -> TransactionEvent:
    """Generate a synthetic transaction event.
    
    Args:
        event_num: Event number (used for tx_id)
        base_ts: Base timestamp in seconds
        fraud_config: Fraud detection configuration (defaults to DEFAULT_FRAUD_CONFIG)
        
    Returns:
        TransactionEvent with synthetic data
    """
    customer_id = f"c{random.randint(1, 5000)}"
    merchant_cat = random.choices(fraud_config.mcc, weights=fraud_config.mcc_weights)[0]
    amount = round(random.lognormvariate(3.2, 0.9), 2)
    
    event_ts = base_ts + event_num
    hour = (event_ts // 3600) % 24
    is_night = hour in fraud_config.night_hours
    
    is_suspicious = (
        amount > fraud_config.threshold_amount
        and merchant_cat in fraud_config.high_value_categories
        and is_night
        and random.random() < fraud_config.probability
    )
    
    return TransactionEvent(
        tx_id=f"tx_{event_num}",
        customer_id=customer_id,
        amount=amount,
        merchant_cat=merchant_cat,
        ts=event_ts,
        label=int(is_suspicious),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Events per second")
    parser.add_argument("--max", type=int, default=DEFAULT_MAX_EVENTS, help="Stop after N events")
    add_streaming_args(parser)
    add_kafka_args(parser)
    
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

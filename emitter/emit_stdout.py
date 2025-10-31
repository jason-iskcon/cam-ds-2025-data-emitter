import argparse
import json
import random
import time

from emitter.contracts import TransactionEvent

MCC = ["grocery", "electronics", "fuel", "luxury", "online"]
MCC_WEIGHTS = [35, 20, 15, 5, 25]
NIGHT_HOURS = {*range(6), *range(23, 24)}
HIGH_VALUE_CATEGORIES = {"luxury", "online"}
LABEL_THRESHOLD_AMOUNT = 300
LABEL_PROBABILITY = 0.4


def synth_event(event_num: int, base_ts: int) -> dict:
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
    
    event = TransactionEvent(
        tx_id=f"tx_{event_num}",
        customer_id=customer_id,
        amount=amount,
        merchant_cat=merchant_cat,
        ts=event_ts,
        label=int(is_suspicious),
    )
    return event.model_dump()


def stream_stdout(rate_per_sec: float = 5.0, max_events: int | None = None):
    """Stream synthetic events to stdout at specified rate."""
    interval = 1.0 / rate_per_sec
    base_ts = int(time.time())
    event_num = 0
    
    while max_events is None or event_num < max_events:
        event = synth_event(event_num, base_ts)
        print(json.dumps(event, ensure_ascii=False))
        event_num += 1
        
        if max_events is None or event_num < max_events:
            time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=5.0, help="Events per second")
    parser.add_argument("--max", type=int, default=10, help="Stop after N events")
    args = parser.parse_args()
    stream_stdout(rate_per_sec=args.rate, max_events=args.max)

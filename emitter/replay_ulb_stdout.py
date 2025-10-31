import argparse
import json
import time

import pandas as pd

from emitter.contracts import TransactionEvent

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


def replay(csv_path: str, rate: float = DEFAULT_RATE, loop: bool = False):
    """Replay ULB credit card dataset events to stdout at specified rate."""
    interval = 1.0 / rate
    base_ts = int(time.time())
    event_num = 0
    
    while True:
        df = pd.read_csv(csv_path)
        for _, row in df.iterrows():
            customer_id = _get_column_value(row, "CustomerID", "") or f"c{event_num % CUSTOMER_ID_MODULO}"
            amount = float(_get_column_value(row, "Amount", "amount", 0.0))
            label = int(_get_column_value(row, "Class", "label", 0))
            
            event = TransactionEvent(
                tx_id=f"ulb_{event_num}",
                customer_id=customer_id,
                amount=amount,
                merchant_cat="unknown",
                ts=base_ts + event_num,
                label=label,
            )
            print(json.dumps(event.model_dump(), ensure_ascii=False))
            event_num += 1
            time.sleep(interval)
        
        if not loop:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=DEFAULT_CSV_PATH, help="Path to CSV file")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Events per second")
    parser.add_argument("--loop", action="store_true", help="Loop replay indefinitely")
    args = parser.parse_args()
    replay(args.path, rate=args.rate, loop=args.loop)

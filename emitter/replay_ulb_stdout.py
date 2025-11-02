import argparse
from typing import Any

import pandas as pd

from emitter._streaming import (
    DEFAULT_BURST_DURATION,
    DEFAULT_BURST_MULTIPLIER,
    add_streaming_args,
    stream_events,
)
from emitter.common import add_kafka_args, create_sink
from emitter.config import BurstConfig
from emitter.contracts import TransactionEvent
from emitter.sinks import Sink

DEFAULT_CSV_PATH = "data/ulb/creditcard.csv"
DEFAULT_RATE = 10.0
CUSTOMER_ID_MODULO = 5000


def _get_column_value(row: pd.Series, primary: str, fallback: str = "", default=None):
    """Get column value with fallback column name."""
    for col in (primary, fallback):
        if col and col in row:
            return row[col]
    return default


def _convert_to_json_safe(val: Any) -> Any:
    """Convert pandas/numpy value to JSON-serializable Python type.

    Args:
        val: Value from pandas Series (may be numpy type, NaN, etc.)

    Returns:
        Native Python type (None for NaN, converted numpy scalars)
    """
    # Check pd.isna() BEFORE hasattr('item') because numpy NaN has .item()
    # but calling .item() on NaN returns Python nan (not None), breaking JSON
    if pd.isna(val):
        return None
    elif hasattr(val, "item"):  # numpy scalar
        return val.item()
    return val


def _extract_features(row: pd.Series, exclude_cols: set[str]) -> dict[str, Any]:
    """Extract feature columns from pandas row, excluding specified columns.

    Args:
        row: Pandas Series representing a data row
        exclude_cols: Set of column names to exclude from features

    Returns:
        Dictionary of feature name to value (JSON-serializable)
    """
    features = {}
    for col in row.index:
        if col not in exclude_cols:
            features[col] = _convert_to_json_safe(row[col])
    return features


class ULBEventGenerator:
    """Generator for ULB credit card dataset events."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df: pd.DataFrame | None = None
        self._load_data()

    def _load_data(self) -> None:
        """Load CSV data."""
        self.df = pd.read_csv(self.csv_path)
        if self.df.empty:
            raise ValueError(f"CSV file {self.csv_path} is empty")

    def __call__(self, event_num: int, base_ts: int) -> TransactionEvent:
        """Generate event from CSV row at position event_num % len(df).

        Includes all columns from the dataset in the features dict for ML compatibility.
        """
        if self.df is None or len(self.df) == 0:
            raise ValueError("No data available")

        row_idx = event_num % len(self.df)
        row = self.df.iloc[row_idx]

        customer_id = (
            _get_column_value(row, "CustomerID", "") or f"c{event_num % CUSTOMER_ID_MODULO}"
        )
        amount = float(_get_column_value(row, "Amount", "amount", 0.0))
        label = int(_get_column_value(row, "Class", "label", 0))

        # Include all columns from the dataset in features for ML compatibility
        # This preserves V1-V28 PCA features, Time, and any other columns needed for ML models
        exclude_cols = {"CustomerID", "Amount", "amount", "Class", "label"}
        features = _extract_features(row, exclude_cols)

        return TransactionEvent(
            tx_id=f"ulb_{event_num}",
            customer_id=customer_id,
            amount=amount,
            merchant_cat="unknown",
            ts=base_ts + event_num,
            label=label,
            features=features,
        )


def replay(
    csv_path: str,
    rate: float = DEFAULT_RATE,
    loop: bool = False,
    sink: Sink | None = None,
    jitter: float = 0.0,
    burst_probability: float = 0.0,
    burst_multiplier: float = DEFAULT_BURST_MULTIPLIER,
    burst_duration_events: int = DEFAULT_BURST_DURATION,
    burst_config: BurstConfig | None = None,
):
    """Replay ULB credit card dataset events to a sink at specified rate.

    Args:
        csv_path: Path to CSV file to replay
        rate: Events per second
        loop: If True, loop indefinitely; otherwise replay once
        sink: Sink to write events to (defaults to StdoutSink if None)
        jitter: Jitter fraction (0.0-1.0) for timing variance
        burst_probability: Probability of burst per event (0.0-1.0).
                          Ignored if burst_config is provided.
        burst_multiplier: Rate multiplier during bursts.
                         Ignored if burst_config is provided.
        burst_duration_events: Number of events in a burst.
                               Ignored if burst_config is provided.
        burst_config: BurstConfig object. If provided, overrides individual burst parameters.
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
        burst_config=burst_config,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=DEFAULT_CSV_PATH, help="Path to CSV file")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="Events per second")
    parser.add_argument("--loop", action="store_true", help="Loop replay indefinitely")
    add_streaming_args(parser)
    add_kafka_args(parser)

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

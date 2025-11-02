"""Common utilities for emitter modules."""

import argparse

from emitter.config import KafkaConfig
from emitter.sinks import KafkaSink, Sink, StdoutSink


def add_kafka_args(parser: argparse.ArgumentParser) -> None:
    """Add Kafka sink configuration arguments to argument parser.

    Args:
        parser: ArgumentParser to add arguments to
    """
    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        help="Kafka broker address (e.g., localhost:9092). If not provided, uses stdout",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        default="transactions",
        help="Kafka topic name (default: transactions)",
    )
    parser.add_argument(
        "--kafka-acks",
        type=str,
        default="1",
        help="Kafka acks setting: '0', '1', 'all' (default: '1')",
    )
    parser.add_argument(
        "--kafka-linger-ms", type=int, default=5, help="Kafka linger_ms (default: 5)"
    )
    parser.add_argument(
        "--kafka-batch-size",
        type=int,
        default=16384,
        help="Kafka batch_size in bytes (default: 16384)",
    )


def create_sink(args: argparse.Namespace) -> Sink:
    """Create appropriate sink based on command-line arguments.

    Args:
        args: Parsed command-line arguments

    Returns:
        Sink instance (StdoutSink or KafkaSink)
    """
    if args.kafka_bootstrap:
        kafka_config = KafkaConfig(
            bootstrap=args.kafka_bootstrap,
            topic=args.kafka_topic,
            acks=args.kafka_acks,
            linger_ms=args.kafka_linger_ms,
            batch_size=args.kafka_batch_size,
        )
        return KafkaSink(config=kafka_config)
    return StdoutSink()

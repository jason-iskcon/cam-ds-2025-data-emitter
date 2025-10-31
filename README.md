# Cambridge Data Science with Machine Learning and AI 2025
## Tiny Data Emitter for Cambridge DS mini-projects
## Purpose

This emitter serves as the data source for downstream projects. It creates synthetic streamed transactions based on downloaded datasets from Kaggle, adding a time dimension to typically static datasets. The purpose is to simulate a realistic data source such as Kafka, enabling downstream systems to process events in real-time or near-real-time streams rather than working with static batch datasets.

## Quickstart

```bash
git clone <your-repo-url>
cd cam-ds-2025-data-emitter

python -m venv .venv && source .venv/bin/activate

pip install -r requirements.txt

# Synthetic 10 events @ 5 eps
make emit

# Get ULB dataset (requires Kaggle CLI configured), then replay @ 10 eps
pip install kaggle
python scripts/kaggle_fetch.py mlg-ulb/creditcardfraud creditcard.csv data/ulb
make replay-ulb

# Optional: Start local Kafka (Redpanda) for testing Kafka sinks
# docker compose up -d
# python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:9092 --kafka-topic transactions
```

## Usage

### Makefile Commands

**`make emit`**
- Emits 10 synthetic transaction events at 5 events per second
- Equivalent to: `python -m emitter.emit_stdout --rate 5 --max 10`
- Simple example for quick testing - generates synthetic transactions with randomized amounts, merchant categories, and fraud labels

**`make emit-realistic`**
- Emits 200 synthetic transaction events at 10 events per second with realistic timing
- Equivalent to: `python -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04`
- **Recommended for realistic testing** - includes jitter (20% timing variance) and occasional bursts (4% chance) to simulate real-world streaming behavior

**`make emit-fast`**
- Emits 50 synthetic transaction events at 20 events per second
- Equivalent to: `python -m emitter.emit_stdout --rate 20 --max 50`
- Faster emission rate for testing downstream systems under higher load

**`make replay-ulb`**
- Replays the ULB credit card fraud dataset at 10 events per second with looping enabled
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop`
- Requires the dataset to be downloaded first (see Quickstart)
- With `--loop`, the dataset cycles indefinitely; without it, replays once through the entire dataset

**`make replay-ulb-realistic`**
- Replays the ULB dataset at 10 events per second with realistic timing and looping
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04`
- **Recommended for realistic ULB testing** - includes jitter and bursts for more authentic streaming behavior

### Basic Script Parameters

**`emitter/emit_stdout.py`**
- `--rate <float>`: Events per second (default: 5.0)
- `--max <int>`: Maximum number of events to emit before stopping (default: 10)
- Examples:
  - `python -m emitter.emit_stdout --rate 10 --max 100`
  - `python -m emitter.emit_stdout --rate 5 --max 50`

**`emitter/replay_ulb_stdout.py`**
- `--path <str>`: Path to CSV file to replay (default: `data/ulb/creditcard.csv`)
- `--rate <float>`: Events per second (default: 10.0)
- `--loop`: If set, loops through the dataset indefinitely; otherwise replays once
- Examples:
  - `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 5`
  - `python -m emitter.replay_ulb_stdout --rate 10 --loop`

**`scripts/kaggle_fetch.py`**
- `dataset`: Kaggle dataset identifier (e.g., `mlg-ulb/creditcardfraud`)
- `filename`: Exact filename or pattern to extract from the dataset
- `outdir`: Output directory where the file will be saved
- Example: `python scripts/kaggle_fetch.py mlg-ulb/creditcardfraud creditcard.csv data/ulb`

### Event Sinks

The emitter supports pluggable **sinks** to route events to different destinations. By default, events are written to stdout, but you can easily switch to Kafka or implement custom sinks.

#### Local Kafka Setup

For local development and testing, the project includes a Docker Compose file for running Redpanda (a Kafka-compatible broker). This allows you to test Kafka functionality without setting up a full Kafka cluster.

**Prerequisites:**
- Docker and Docker Compose installed
- `kafka-python` package: `pip install kafka-python`

**Starting Redpanda:**
```bash
# Start Redpanda broker
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f redpanda

# Stop the broker
docker compose down
```

**Using with the emitter:**
Once running, Redpanda will be available at `localhost:9092`. Topics are automatically created when first used. Example:

```bash
python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:9092 --kafka-topic transactions
```

**Monitoring:**
- Redpanda console: `http://localhost:9644` - Web UI for monitoring topics, messages, and broker metrics
- Port 9092: Kafka-compatible API endpoint

#### Available Sinks

**StdoutSink** (default)
- Writes events as JSON lines to stdout
- No additional dependencies
- Example: `python -m emitter.emit_stdout --rate 5 --max 10`

**KafkaSink**
- Writes events to a Kafka topic
- Requires `kafka-python` package: `pip install kafka-python`
- Automatically handles serialization, batching, and connection management
- Example: `python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:9092 --kafka-topic transactions`

#### Custom Sinks

You can implement custom sinks by creating a class with a `write(event: dict[str, Any]) -> None` method. The sink must conform to the `Sink` protocol:

```python
from emitter.sinks import Sink

class MyCustomSink:
    def write(self, event: dict[str, Any]) -> None:
        # Your custom logic here
        pass
```

#### Sink Configuration

**Kafka Configuration:**
- `--kafka-bootstrap`: Broker address (required for Kafka)
- `--kafka-topic`: Topic name (default: `transactions`)
- `--kafka-acks`: Acknowledgment mode - `'0'` (no ack), `'1'` (leader), `'all'` (all replicas)
- `--kafka-linger-ms`: Wait time before sending a batch (default: 5ms)
- `--kafka-batch-size`: Batch size in bytes (default: 16384)

**Sink Examples:**
```bash
# Emit to stdout (default)
python -m emitter.emit_stdout --rate 5 --max 20

# Emit to Kafka topic
python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:9092 --kafka-topic transactions

# High-throughput Kafka with custom configuration
python -m emitter.emit_stdout --rate 50 --max 1000 --kafka-bootstrap localhost:9092 --kafka-topic high-volume --kafka-acks all --kafka-batch-size 32768
```

## Advanced Features

### Realistic Timing Features

The emitter supports **jitter** and **bursts** to simulate realistic streaming data sources:

- **Jitter**: Adds timing variance to inter-event intervals using Gaussian noise. This mimics network latency and processing delays found in real systems. Use `--jitter 0.15-0.25` for moderate variance.

- **Bursts**: Randomly triggers periods of faster emission to simulate traffic spikes (e.g., flash sales, peak hours). Use `--burst-prob 0.03-0.05` for occasional bursts (3-5% chance per event). During bursts, events emit at `rate × burst_multiplier` for `burst_duration` events, then return to normal rate.

### Advanced Script Parameters

Both `emitter/emit_stdout.py` and `emitter/replay_ulb_stdout.py` support the following advanced parameters:

**Timing Parameters:**
- `--jitter <float>`: Jitter fraction (0.0-1.0) for timing variance. Adds Gaussian noise to inter-event intervals for more realistic timing. 0.2 = ±20% timing variation (default: 0.0)
- `--burst-prob <float>`: Probability of entering a burst per event (0.0-1.0). Bursts temporarily increase emission rate for realistic traffic spikes (default: 0.0)
- `--burst-mult <float>`: Rate multiplier during bursts. 5.0 means events emit at 5x the base rate during bursts (default: 5.0)
- `--burst-duration <int>`: Number of events to emit during a burst (default: 10)

### Testing Examples

Here are example commands to test the jitter and burst features:

**Jitter Testing:**
```bash
# Moderate jitter - 20% timing variance
python -m emitter.emit_stdout --rate 5 --max 30 --jitter 0.2

# High jitter - 30% timing variance (more chaotic)
python -m emitter.emit_stdout --rate 10 --max 50 --jitter 0.3

# Low jitter - 10% timing variance (subtle variation)
python -m emitter.emit_stdout --rate 5 --max 20 --jitter 0.1
```

**Burst Testing:**
```bash
# Frequent bursts - 10% chance per event
python -m emitter.emit_stdout --rate 5 --max 100 --burst-prob 0.1

# Occasional bursts - 3% chance per event (realistic)
python -m emitter.emit_stdout --rate 10 --max 200 --burst-prob 0.03

# Very aggressive bursts - 10x multiplier, 20 events long
python -m emitter.emit_stdout --rate 5 --max 100 --burst-prob 0.05 --burst-mult 10.0 --burst-duration 20

# Short, frequent bursts - 2% chance, 5 events long
python -m emitter.emit_stdout --rate 10 --max 150 --burst-prob 0.02 --burst-duration 5
```

**Combined Jitter + Burst:**
```bash
# Realistic streaming with both jitter and bursts
python -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04

# High variance scenario - lots of jitter + frequent bursts
python -m emitter.emit_stdout --rate 5 --max 100 --jitter 0.25 --burst-prob 0.08 --burst-mult 7.0

# Subtle realism - low jitter, rare bursts
python -m emitter.emit_stdout --rate 8 --max 150 --jitter 0.1 --burst-prob 0.02
```

**ULB Dataset with Timing Features:**
```bash
# Replay with moderate jitter
python -m emitter.replay_ulb_stdout --rate 10 --jitter 0.15

# Replay with bursts enabled
python -m emitter.replay_ulb_stdout --rate 10 --burst-prob 0.03 --burst-duration 15

# Realistic streaming: jitter + bursts, looping indefinitely
python -m emitter.replay_ulb_stdout --rate 10 --loop --jitter 0.2 --burst-prob 0.04

# High-throughput test with aggressive bursts
python -m emitter.replay_ulb_stdout --rate 20 --jitter 0.15 --burst-prob 0.05 --burst-mult 8.0 --burst-duration 25

# Replay ULB dataset to Kafka with realistic timing
python -m emitter.replay_ulb_stdout --rate 10 --loop --kafka-bootstrap localhost:9092 --kafka-topic ulb-fraud --jitter 0.2 --burst-prob 0.04
```

**Observing the Effects:**
- Watch timing: Events should arrive with variable intervals when jitter is enabled
- Watch for bursts: You'll see rapid-fire events followed by normal rate when bursts occur
- Compare rates: Try the same command with and without jitter/bursts to see the difference
- Use with pipes: `python -m emitter.emit_stdout --rate 5 --max 50 --jitter 0.2 | head -20` to see first 20 events

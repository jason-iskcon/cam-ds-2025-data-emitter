# Cambridge Data Science with ML and AI 2025/6
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
# make setup
# make emit-kafka
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
- **Preserves all ML features**: All columns from the dataset (V1-V28 PCA features, Time, etc.) are included in the `features` field for ML model compatibility
- With `--loop`, the dataset cycles indefinitely; without it, replays once through the entire dataset

**`make replay-ulb-realistic`**
- Replays the ULB dataset at 10 events per second with realistic timing and looping
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04`
- **Recommended for realistic ULB testing** - includes jitter and bursts for more authentic streaming behavior
- All ML features are preserved in the `features` field

**Kafka Emit Targets:**

**`make emit-kafka`**
- Emits 10 synthetic transaction events to Kafka at 5 events per second
- Requires Redpanda cluster to be running (`make up` or `make setup`)
- Automatically checks cluster status before emitting
- Equivalent to: `python -m emitter.emit_stdout --rate 5 --max 10 --kafka-bootstrap localhost:19092 --kafka-topic transactions`

**`make emit-kafka-realistic`**
- Emits 200 synthetic events to Kafka at 10 events per second with realistic timing
- Includes jitter and bursts for authentic streaming behavior
- Equivalent to: `python -m emitter.emit_stdout --rate 10 --max 200 --jitter 0.2 --burst-prob 0.04 --kafka-bootstrap localhost:19092 --kafka-topic transactions`

**`make replay-ulb-kafka`**
- Replays the ULB dataset to Kafka at 10 events per second with looping
- Requires Redpanda cluster running and dataset downloaded
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --kafka-bootstrap localhost:19092 --kafka-topic transactions`

**`make replay-ulb-kafka-realistic`**
- Replays the ULB dataset to Kafka with realistic timing and looping
- Includes jitter and bursts for more authentic behavior
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop --jitter 0.2 --burst-prob 0.04 --kafka-bootstrap localhost:19092 --kafka-topic transactions`

**Cluster and Topic Management:**

**`make setup`**
- One-command setup: starts Redpanda cluster, waits for readiness, creates `transactions` topic, and shows cluster info
- Recommended first step when setting up Kafka testing

**`make up` / `make down`**
- Start/stop the Redpanda cluster
- Equivalent to: `docker compose up -d` / `docker compose down`

**`make status`**
- Check container status
- Equivalent to: `docker compose ps`

**`make logs`**
- View cluster logs in follow mode
- Equivalent to: `docker compose logs -f redpanda`

**`make cluster-info`**
- Show cluster information and health
- Equivalent to: `docker compose exec redpanda rpk cluster info`

**`make topics`**
- List all topics in the cluster
- Equivalent to: `docker compose exec redpanda rpk topic list`

**`make topic-create`**
- Create the default `transactions` topic
- Equivalent to: `docker compose exec redpanda rpk topic create transactions`

**`make topic-describe`**
- Describe the `transactions` topic with partition details
- Equivalent to: `docker compose exec redpanda rpk topic describe transactions -p`

**`make topic-delete`**
- Delete the `transactions` topic
- Equivalent to: `docker compose exec redpanda rpk topic delete transactions`

**`make topic-consume`**
- Consume last 10 messages from the `transactions` topic
- Equivalent to: `docker compose exec redpanda rpk topic consume transactions -n 10 --format '%v\n'`

**`make topic-consume-start`**
- Consume from the beginning of the `transactions` topic (last 10 messages)
- Useful for verifying messages were written
- Equivalent to: `docker compose exec redpanda rpk topic consume transactions --offset start -n 10 --format '%v\n'`

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

#### Local Kafka Setup with Redpanda

For local development and testing, the project includes a Docker Compose file for running Redpanda (a Kafka-compatible broker). This allows you to test Kafka functionality without setting up a full Kafka cluster.

**Prerequisites:**
- Docker and Docker Compose installed
- `confluent-kafka` package (installed via `requirements.txt`)

**Starting Redpanda:**

Using Makefile (recommended):
```bash
# One-command setup: start cluster, create topic, show info
make setup

# Or manually:
make up          # Start Redpanda broker
make status      # Check status
make logs        # View logs (follow mode)
make down        # Stop the broker
```

Using Docker Compose directly:
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

**Network Configuration:**
Redpanda is configured with dual listeners for proper Docker networking:
- **Internal network** (containers): `redpanda:9092` - Use this from within Docker containers
- **External (host)**: `localhost:19092` - Use this from your host machine

**Using with the emitter:**

**From host machine (local Python):**

Using Makefile (recommended):
```bash
# Quick emit to Kafka (requires cluster running)
make emit-kafka

# Realistic timing with jitter and bursts
make emit-kafka-realistic

# Replay ULB dataset to Kafka
make replay-ulb-kafka

# Replay ULB with realistic timing
make replay-ulb-kafka-realistic
```

Using Python directly:
```bash
python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:19092 --kafka-topic transactions
```

**From Docker container:**
```bash
# Emit synthetic events (no volume mount needed)
docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
  cam-ds-2025-data-emitter:dev \
  python -m emitter.emit_stdout --rate 10 --max 100 \
  --kafka-bootstrap redpanda:9092 --kafka-topic transactions

# Replay CSV files (volume mount required for data access)
docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
  -v C:/Workspace/cam-ds-2025-data-emitter/data:/app/data \
  cam-ds-2025-data-emitter:dev \
  python -m emitter.replay_ulb_stdout --rate 10 --loop \
  --kafka-bootstrap redpanda:9092 --kafka-topic transactions
```

**Note on Volume Mounts:**
- The `-v` flag mounts your local `data/` directory into the container at `/app/data`
- **Required** when using `replay_ulb_stdout.py` to access CSV files
- **Not needed** when using `emit_stdout.py` (generates synthetic data)
- Windows path format: `C:/Workspace/cam-ds-2025-data-emitter/data:/app/data`
- Linux/Mac path format: `/path/to/project/data:/app/data`

**Verifying Messages are Received:**

Using Makefile (recommended):
```bash
# List all topics
make topics

# Describe transactions topic with partition details
make topic-describe

# Consume last 10 messages
make topic-consume

# Consume from beginning (verify messages were written)
make topic-consume-start
```

Using Docker directly:
```bash
# View topic details and offsets
docker exec cam-ds-2025-data-emitter-redpanda-1 rpk topic describe transactions -p

# Consume messages directly
docker exec cam-ds-2025-data-emitter-redpanda-1 rpk topic consume transactions -n 5 --format '%v\n'

# Check high-watermark (proves messages were written)
docker exec cam-ds-2025-data-emitter-redpanda-1 rpk topic describe transactions -p | grep HIGH-WATERMARK
```

**Monitoring:**
- Redpanda Console: `http://localhost:8080` - Modern web UI for exploring topics, messages, consumers, and cluster health (included in docker-compose)
- Redpanda admin console: `http://localhost:9644` - Web UI for monitoring topics, messages, and broker metrics
- Port 19092: External Kafka-compatible API endpoint (from host)
- Port 9092: Internal Kafka API endpoint (from containers)

**Troubleshooting:**

If messages aren't being received, verify step-by-step:

1. **Check Redpanda is running:**
   ```bash
   # Using Makefile:
   make status
   make logs
   
   # Or using Docker directly:
   docker compose ps
   docker compose logs redpanda
   ```

2. **Verify network connectivity (from container):**
   ```bash
   docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
     cam-ds-2025-data-emitter:dev \
     python -c "import socket; s = socket.socket(); s.connect(('redpanda', 9092)); print('Connected'); s.close()"
   ```

3. **Test topic produce/consume directly:**
   ```bash
   # Produce test message
   docker exec cam-ds-2025-data-emitter-redpanda-1 \
     bash -c "echo 'test' | rpk topic produce transactions"
   
   # Consume to verify
   docker exec cam-ds-2025-data-emitter-redpanda-1 \
     rpk topic consume transactions -n 1
   ```

4. **Check topic offsets (proves messages were written):**
   ```bash
   # Using Makefile:
   make topic-describe
   
   # Or using Docker directly:
   docker exec cam-ds-2025-data-emitter-redpanda-1 \
     rpk topic describe transactions -p
   ```
   Look for `HIGH-WATERMARK` - this shows how many messages have been written.

5. **Common issues:**
   - **Using wrong port**: Host machine should use `localhost:19092`, containers should use `redpanda:9092`
   - **Network not connected**: Docker containers must use `--network cam-ds-2025-data-emitter_redpanda-net`
   - **Topic doesn't exist**: Topics are auto-created, but verify with `make topics` or `rpk topic list`
   - **No messages consumed**: Use `--offset start` to read from beginning, or check if you're consuming from the latest offset

#### Available Sinks

**StdoutSink** (default)
- Writes events as JSON lines to stdout
- No additional dependencies
- Example: `python -m emitter.emit_stdout --rate 5 --max 10`

**KafkaSink**
- Writes events to a Kafka topic using `confluent-kafka` (production-grade client)
- Requires `confluent-kafka` package (installed via `requirements.txt`)
- Automatically handles serialization, batching, and connection management
- Example (from host): `python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:19092 --kafka-topic transactions`
- Example (from Docker): Use `redpanda:9092` and connect container to `cam-ds-2025-data-emitter_redpanda-net` network

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

Using Makefile (recommended for common operations):
```bash
# Emit to stdout (default)
make emit
make emit-realistic

# Emit to Kafka topic (requires cluster running)
make emit-kafka
make emit-kafka-realistic

# Replay ULB dataset to Kafka
make replay-ulb-kafka
make replay-ulb-kafka-realistic
```

Using Python directly (for custom configurations):
```bash
# Emit to stdout (default)
python -m emitter.emit_stdout --rate 5 --max 20

# Emit to Kafka topic (from host machine)
python -m emitter.emit_stdout --rate 10 --max 100 --kafka-bootstrap localhost:19092 --kafka-topic transactions

# Emit to Kafka from Docker container
docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
  cam-ds-2025-data-emitter:dev \
  python -m emitter.emit_stdout --rate 10 --max 100 \
  --kafka-bootstrap redpanda:9092 --kafka-topic transactions

# High-throughput Kafka with custom configuration
python -m emitter.emit_stdout --rate 50 --max 1000 \
  --kafka-bootstrap localhost:19092 --kafka-topic high-volume \
  --kafka-acks all --kafka-batch-size 32768

# Replay ULB dataset to Kafka (note: -v mounts data directory)
docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
  -v C:/Workspace/cam-ds-2025-data-emitter/data:/app/data \
  cam-ds-2025-data-emitter:dev \
  python -m emitter.replay_ulb_stdout --rate 10 --loop \
  --kafka-bootstrap redpanda:9092 --kafka-topic transactions
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

# Replay ULB dataset to Kafka with realistic timing (from host)
# Using Makefile:
make replay-ulb-kafka-realistic

# Or using Python directly:
python -m emitter.replay_ulb_stdout --rate 10 --loop --kafka-bootstrap localhost:19092 --kafka-topic ulb-fraud --jitter 0.2 --burst-prob 0.04

# Replay ULB dataset to Kafka from Docker
docker run --rm --network cam-ds-2025-data-emitter_redpanda-net \
  -v C:/Workspace/cam-ds-2025-data-emitter/data:/app/data \
  cam-ds-2025-data-emitter:dev \
  python -m emitter.replay_ulb_stdout --rate 10 --loop \
  --kafka-bootstrap redpanda:9092 --kafka-topic ulb-fraud \
  --jitter 0.2 --burst-prob 0.04
```

**Observing the Effects:**
- Watch timing: Events should arrive with variable intervals when jitter is enabled
- Watch for bursts: You'll see rapid-fire events followed by normal rate when bursts occur
- Compare rates: Try the same command with and without jitter/bursts to see the difference
- Use with pipes: `python -m emitter.emit_stdout --rate 5 --max 50 --jitter 0.2 | head -20` to see first 20 events

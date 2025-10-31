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
```

## Usage

### Makefile Commands

**`make emit`**
- Emits 10 synthetic transaction events at 5 events per second
- Equivalent to: `python -m emitter.emit_stdout --rate 5 --max 10`
- Generates synthetic transactions with randomized amounts, merchant categories, and fraud labels

**`make emit-fast`**
- Emits 50 synthetic transaction events at 20 events per second
- Equivalent to: `python -m emitter.emit_stdout --rate 20 --max 50`
- Faster emission rate for testing downstream systems under higher load

**`make replay-ulb`**
- Replays the ULB credit card fraud dataset at 10 events per second with looping enabled
- Equivalent to: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 10 --loop`
- Requires the dataset to be downloaded first (see Quickstart)
- With `--loop`, the dataset cycles indefinitely; without it, replays once through the entire dataset

### Script Parameters

**`emitter/emit_stdout.py`**
- `--rate <float>`: Events per second (default: 5.0)
- `--max <int>`: Maximum number of events to emit before stopping (default: 10)
- `--jitter <float>`: Jitter fraction (0.0-1.0) for timing variance. Adds Gaussian noise to inter-event intervals for more realistic timing. 0.2 = ±20% timing variation (default: 0.0)
- `--burst-prob <float>`: Probability of entering a burst per event (0.0-1.0). Bursts temporarily increase emission rate for realistic traffic spikes (default: 0.0)
- `--burst-mult <float>`: Rate multiplier during bursts. 5.0 means events emit at 5x the base rate during bursts (default: 5.0)
- `--burst-duration <int>`: Number of events to emit during a burst (default: 10)
- Examples:
  - `python -m emitter.emit_stdout --rate 10 --max 100`
  - `python -m emitter.emit_stdout --rate 5 --max 50 --jitter 0.2 --burst-prob 0.05`

**`emitter/replay_ulb_stdout.py`**
- `--path <str>`: Path to CSV file to replay (default: `data/ulb/creditcard.csv`)
- `--rate <float>`: Events per second (default: 10.0)
- `--loop`: If set, loops through the dataset indefinitely; otherwise replays once
- `--jitter <float>`: Jitter fraction (0.0-1.0) for timing variance (default: 0.0)
- `--burst-prob <float>`: Probability of entering a burst per event (0.0-1.0) (default: 0.0)
- `--burst-mult <float>`: Rate multiplier during bursts (default: 5.0)
- `--burst-duration <int>`: Number of events to emit during a burst (default: 10)
- Examples:
  - `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 5`
  - `python -m emitter.replay_ulb_stdout --rate 10 --loop --jitter 0.15 --burst-prob 0.03`

**`scripts/kaggle_fetch.py`**
- `dataset`: Kaggle dataset identifier (e.g., `mlg-ulb/creditcardfraud`)
- `filename`: Exact filename or pattern to extract from the dataset
- `outdir`: Output directory where the file will be saved
- Example: `python scripts/kaggle_fetch.py mlg-ulb/creditcardfraud creditcard.csv data/ulb`

### Realistic Timing Features

The emitter supports **jitter** and **bursts** to simulate realistic streaming data sources:

- **Jitter**: Adds timing variance to inter-event intervals using Gaussian noise. This mimics network latency and processing delays found in real systems. Use `--jitter 0.15-0.25` for moderate variance.

- **Bursts**: Randomly triggers periods of faster emission to simulate traffic spikes (e.g., flash sales, peak hours). Use `--burst-prob 0.03-0.05` for occasional bursts (3-5% chance per event). During bursts, events emit at `rate × burst_multiplier` for `burst_duration` events, then return to normal rate.

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
```

**Observing the Effects:**
- Watch timing: Events should arrive with variable intervals when jitter is enabled
- Watch for bursts: You'll see rapid-fire events followed by normal rate when bursts occur
- Compare rates: Try the same command with and without jitter/bursts to see the difference
- Use with pipes: `python -m emitter.emit_stdout --rate 5 --max 50 --jitter 0.2 | head -20` to see first 20 events


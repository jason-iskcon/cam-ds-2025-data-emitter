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
- Example: `python -m emitter.emit_stdout --rate 10 --max 100`

**`emitter/replay_ulb_stdout.py`**
- `--path <str>`: Path to CSV file to replay (default: `data/ulb/creditcard.csv`)
- `--rate <float>`: Events per second (default: 10.0)
- `--loop`: If set, loops through the dataset indefinitely; otherwise replays once
- Example: `python -m emitter.replay_ulb_stdout --path data/ulb/creditcard.csv --rate 5`

**`scripts/kaggle_fetch.py`**
- `dataset`: Kaggle dataset identifier (e.g., `mlg-ulb/creditcardfraud`)
- `filename`: Exact filename or pattern to extract from the dataset
- `outdir`: Output directory where the file will be saved
- Example: `python scripts/kaggle_fetch.py mlg-ulb/creditcardfraud creditcard.csv data/ulb`


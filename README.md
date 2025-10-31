# Cambridge Data Science with Machine Learning and AI 2025
## Tiny Data Emitter for Cambridge DS mini-projects.

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
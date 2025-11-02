# Usage: python kaggle_fetch.py <dataset> <filename> <outdir>
# Examples:
# python scripts/kaggle_fetch.py mlg-ulb/creditcardfraud creditcard.csv data/ulb
# python scripts/kaggle_fetch.py ealaxi/paysim1 "PS_20174392719_1491204439457_log.csv" data/paysim

import argparse
import glob
import json
import os
import pathlib
import shutil
import subprocess
import sys


def find_kaggle_exe():
    """Find kaggle executable in venv Scripts/bin or PATH."""
    if sys.executable:
        scripts_dir = pathlib.Path(sys.executable).parent
        for exe_name in ["kaggle.exe", "kaggle"]:
            exe_path = scripts_dir / exe_name
            if exe_path.exists():
                return str(exe_path)

    return shutil.which("kaggle") or shutil.which("kaggle.exe")


def load_kaggle_credentials():
    """Load kaggle credentials from .kaggle/kaggle.json."""
    kaggle_json = pathlib.Path(__file__).parent.parent / ".kaggle" / "kaggle.json"

    try:
        with open(kaggle_json) as f:
            config = json.load(f)
        if not config.get("username") or not config.get("key"):
            raise ValueError("kaggle.json must contain 'username' and 'key' fields")
        return config["username"], config["key"]
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", help="e.g. mlg-ulb/creditcardfraud")
    parser.add_argument("filename", help="exact file to extract, e.g. creditcard.csv or PS_*.csv")
    parser.add_argument("outdir", help="where to place the file, e.g. data/ulb")
    args = parser.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    tmp = outdir / "_tmp"
    tmp.mkdir(exist_ok=True)

    kaggle_cmd = find_kaggle_exe()
    if not kaggle_cmd:
        print(
            "ERROR: kaggle executable not found. Install with: pip install kaggle", file=sys.stderr
        )
        sys.exit(1)

    username, key = load_kaggle_credentials()
    env = os.environ.copy()
    env["KAGGLE_USERNAME"] = username
    env["KAGGLE_KEY"] = key

    try:
        subprocess.check_call(
            [kaggle_cmd, "datasets", "download", "-d", args.dataset, "-p", str(tmp), "--unzip"],
            env=env,
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to download dataset: {e}", file=sys.stderr)
        sys.exit(1)

    matches = glob.glob(str(tmp / args.filename))
    if not matches:
        print(f"ERROR: file pattern {args.filename} not found in {args.dataset}", file=sys.stderr)
        sys.exit(1)

    for match in matches:
        shutil.move(match, outdir / pathlib.Path(match).name)

    for path in tmp.glob("*"):
        path.unlink()
    tmp.rmdir()
    print(f"Saved to {outdir}")

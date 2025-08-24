#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import time
import hashlib
import logging
from datetime import datetime
from dateutil import tz

import requests
import pandas as pd

# --- Project base config ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Logging
log_path = os.path.join(LOG_DIR, f"collect_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# Data sources (main OWID dataset)
SOURCES = {
    "owid_covid_data_github": "https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv"
}

def timestamp_paris() -> str:
    """Return a timestamp YYYYMMDD_HHMMSS in Europe/Paris timezone."""
    paris = tz.gettz("Europe/Paris")
    return datetime.now(tz=paris).strftime("%Y%m%d_%H%M%S")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def download_csv(url: str, timeout=60) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content

def validate_csv_bytes(b: bytes) -> bool:
    """Basic validation: non-empty data + readable by pandas."""
    if len(b) < 1000:  # avoid empty/HTML error responses
        return False
    try:
        from io import BytesIO
        _ = pd.read_csv(BytesIO(b), nrows=10)
        return True
    except Exception:
        return False

def save_bytes_to_file(b: bytes, name: str) -> str:
    ts = timestamp_paris()
    path = os.path.join(DATA_DIR, f"{name}_{ts}.csv")
    with open(path, "wb") as f:
        f.write(b)
    return path

def quick_profile_csv(path: str) -> dict:
    """Quick profile: number of rows/columns and check for key fields."""
    df = pd.read_csv(path)
    profile = {
        "rows": len(df),
        "cols": len(df.columns),
        "has_date": "date" in df.columns,
        "has_location": "location" in df.columns,
        "columns_sample": df.columns[:10].tolist()
    }
    return profile

def main():
    logging.info("=== START OWID COVID COLLECTION ===")
    started = time.time()
    results = []

    for name, url in SOURCES.items():
        try:
            logging.info(f"Downloading: {name} <- {url}")
            b = download_csv(url)
            digest = sha256_bytes(b)
            logging.info(f"Checksum SHA-256: {digest[:16]}…")

            if not validate_csv_bytes(b):
                logging.error(f"Validation failed for {name} (invalid content).")
                continue

            out_path = save_bytes_to_file(b, name)
            prof = quick_profile_csv(out_path)
            logging.info(f"Saved: {out_path} | Profile: {prof}")

            results.append({
                "name": name,
                "path": out_path,
                "sha256": digest,
                "profile": prof
            })

        except Exception as e:
            logging.exception(f"Error while processing {name}: {e}")

    elapsed = time.time() - started
    logging.info(f"=== END COLLECTION (ok={len(results)}) in {elapsed:.1f}s ===")

    # Write summary to CSV (append mode)
    recap_path = os.path.join(LOG_DIR, "collect_history.csv")
    fieldnames = ["ts_paris", "name", "path", "sha256", "rows", "cols"]
    exists = os.path.exists(recap_path)
    with open(recap_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        for r in results:
            w.writerow({
                "ts_paris": timestamp_paris(),
                "name": r["name"],
                "path": r["path"],
                "sha256": r["sha256"],
                "rows": r["profile"]["rows"],
                "cols": r["profile"]["cols"],
            })

if __name__ == "__main__":
    main()

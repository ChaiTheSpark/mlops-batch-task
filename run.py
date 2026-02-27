import argparse
import logging
import yaml
import pandas as pd
import numpy as np
import json
import sys
import time
from pathlib import Path


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def write_metrics(output_path, metrics):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def load_config(config_path):
    if not Path(config_path).exists():
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

    return config


def load_data(input_path):
    if not Path(input_path).exists():
        raise FileNotFoundError("Input CSV file not found")

    try:
        df = pd.read_csv(input_path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("Input CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    args = parser.parse_args()

    setup_logging(args.log_file)

    start_time = time.time()

    try:
        logging.info("Job started")

        # Load config
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # Load data
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # Rolling mean
        logging.info("Computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # Signal (exclude NaNs)
        logging.info("Generating signal")
        df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)
        df = df.dropna()

        rows_processed = len(df)
        signal_rate = float(df["signal"].mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        write_metrics(args.output, metrics)

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e),
        }

        write_metrics(args.output, error_metrics)
        logging.error(f"Error occurred: {e}")
        print(json.dumps(error_metrics, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
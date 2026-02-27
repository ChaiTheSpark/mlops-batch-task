# MLOps Batch Job – Technical Assessment

## Overview
This project implements a minimal MLOps-style batch job in Python.

Features:
- Deterministic execution via YAML config + seed
- Structured logging
- Machine-readable metrics JSON
- Dockerized execution

---

## Local Run

```bash
pip install -r requirements.txt

python run.py \
  --input data.csv \
  --config config.yaml \
  --output metrics.json \
  --log-file run.log
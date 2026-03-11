from __future__ import annotations
import logging
from pathlib import Path
from typing import Iterator
import pandas as pd

log = logging.getLogger(__name__)

def read_file(cfg: dict) -> Iterator[dict]:
    path = Path(cfg["path"])
    fmt = cfg.get("fmt", "csv")
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}\nRun: python scripts/generate_csv.py")
    log.info("Reading %s file: %s", fmt, path)
    if fmt == "csv":
        df = pd.read_csv(path, dtype=str)
    elif fmt == "parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    log.info("File rows read: %d", len(df))
    for row in df.to_dict(orient="records"):
        yield row

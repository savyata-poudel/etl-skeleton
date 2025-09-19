from __future__ import annotations
import pandas as pd
from pathlib import Path

def read_file(path: str, fmt: str = "csv") -> pd.DataFrame:
    p=Path(path)
    if fmt == "csv":
        return pd.read_csv(p)
    if fmt == "parquet":
        return pd.read_parquet(p)
    raise ValueError(f"Unsupported format: {fmt}")
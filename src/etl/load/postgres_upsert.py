from __future__ import annotations
import logging
from typing import Iterator
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

def upsert_rows(dsn: str, table: str, rows: list[dict], key_cols: list[str]) -> int:
    if not rows:
        log.warning("upsert_rows called with empty list - skipping")
        return 0
    engine = create_engine(dsn)
    cols = None
    total = 0
    with engine.begin() as cx:
        for batch in _chunks(rows, 500):
            if cols is None:
                cols = list(batch[0].keys())
            placeholders = ", ".join(f":{c}" for c in cols)
            conflict_cols = ", ".join(key_cols)
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key_cols)
            sql = f"""
                INSERT INTO {table} ({", ".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {updates}
            """
            cx.execute(text(sql), batch)
            total += len(batch)
            log.info("Upserted batch: table=%s rows=%d", table, len(batch))
    log.info("UPSERT complete: table=%s total=%d", table, total)
    return total

def _chunks(seq: list, size: int) -> Iterator[list]:
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

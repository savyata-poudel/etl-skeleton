from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Iterator, Mapping
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

def connect(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)

def get_watermark(engine: Engine, source_name: str) -> datetime:
    with engine.connect() as cx:
        row = cx.execute(text("SELECT last_run_at FROM public.etl_watermarks WHERE source_name = :s"), {"s": source_name}).fetchone()
    if row:
        ts = row[0]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return _EPOCH

def set_watermark(engine: Engine, source_name: str, ts: datetime | None = None) -> None:
    ts = ts or datetime.now(timezone.utc)
    with engine.begin() as cx:
        cx.execute(text("""
            INSERT INTO public.etl_watermarks (source_name, last_run_at, updated_at)
            VALUES (:s, :ts, NOW())
            ON CONFLICT (source_name)
            DO UPDATE SET last_run_at = EXCLUDED.last_run_at, updated_at = NOW()
        """), {"s": source_name, "ts": ts})

def read_in_chunks(engine: Engine, sql: str, params: Mapping, chunk_size: int = 500) -> Iterator[dict]:
    offset = 0
    while True:
        with engine.connect() as cx:
            rows = cx.execute(text(sql), {**params, "limit": chunk_size, "offset": offset}).mappings().all()
        if not rows:
            break
        log.info("DB chunk: offset=%d rows=%d", offset, len(rows))
        for r in rows:
            yield dict(r)
        offset += chunk_size

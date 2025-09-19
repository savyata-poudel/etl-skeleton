from __future__ import annotations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Iterator, Mapping

def connect(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)

def read_in_chunks(engine: Engine, sql: str, params: Mapping, chunk_size: int = 1000) -> Iterator[list[dict]]:
    offset=0
    while True:
        with engine.connect() as cx:
            rows = cx.execute(text(sql), {**params, "limit": chunk_size, "offset": offset}).mappings().all()
        if not rows:
            break
        yield[dict(r) for r in rows]
        offset += chunk_size
    
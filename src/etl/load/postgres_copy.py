from __future__ import annotations
import io, logging
import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

def copy_dataframe(dsn: str, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        log.warning("copy_dataframe called with empty DataFrame - skipping")
        return 0
    engine = create_engine(dsn)
    with engine.begin() as cx:
        cx.execute(text(f"CREATE TEMP TABLE _stg_copy AS TABLE {table} WITH NO DATA"))
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        raw_conn = cx.connection
        cols = ", ".join(df.columns)
        with raw_conn.cursor() as cur:
            cur.copy_expert(f"COPY _stg_copy ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", buf)
        result = cx.execute(text(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _stg_copy ON CONFLICT DO NOTHING"))
        inserted = result.rowcount
    log.info("COPY load complete: table=%s inserted=%d", table, inserted)
    return inserted

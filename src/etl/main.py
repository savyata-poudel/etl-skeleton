from __future__ import annotations
import argparse, logging, sys
from datetime import datetime, timezone
import pandas as pd
from etl import config
from etl.logging_setup import setup_logging
from etl.extract import file_reader, api_client, db_reader
from etl.transform.core import RunMetrics, normalize_api_users, normalize_file_users, normalize_db_users
from etl.load.postgres_copy import copy_dataframe
from etl.load.postgres_upsert import upsert_rows

log = logging.getLogger(__name__)

def run(source: str, load_mode: str, incremental: bool = False) -> None:
    setup_logging()
    cfg = config.load_settings()
    dsn = config.env("POSTGRES_DSN")
    target_table = cfg["run"]["target_table"]
    key_cols = cfg["load"]["key_column"]
    metrics = RunMetrics()
    run_start = datetime.now(timezone.utc)
    log.info("ETL starting", extra={"source": source, "load_mode": load_mode})

    if source == "file":
        raw_iter = file_reader.read_file(cfg["sources"]["file"])
        users = list(normalize_file_users(raw_iter, metrics))
    elif source == "api":
        base_url = config.env("API_BASE_URL")
        token = config.env("API_TOKEN")
        api_cfg = cfg["sources"]["api"]
        client = api_client.ApiClient(base_url=base_url, token=token)
        raw_iter = client.paginate(endpoint=api_cfg["endpoint"], page_param=api_cfg["page_param"],
                                   per_page_param=api_cfg["per_page_param"], per_page=int(api_cfg["per_page"]),
                                   data_key=api_cfg.get("data_key","data"))
        users = list(normalize_api_users(raw_iter, metrics))
    elif source == "db":
        engine = db_reader.connect(dsn)
        db_cfg = cfg["sources"]["db"]
        since = db_reader.get_watermark(engine, "db") if incremental else datetime(1970,1,1,tzinfo=timezone.utc)
        log.info("DB extract since=%s", since.isoformat())
        raw_iter = db_reader.read_in_chunks(engine=engine, sql=db_cfg["query"],
                                            params={"since": since}, chunk_size=int(cfg["run"]["batch_size"]))
        users = list(normalize_db_users(raw_iter, metrics))
    else:
        raise SystemExit(f"Unknown source: {source!r}")

    log.info("Extract+transform done", extra=metrics.summary())

    if not users:
        log.warning("No valid rows - nothing to load")
        metrics.loaded_count = 0
        _log_final(metrics, source, load_mode, run_start)
        return

    if source == "api":
        rows = [{"id":u.id,"email":u.email,"first_name":u.first_name,"last_name":u.last_name,"avatar":u.avatar,"source":u.source} for u in users]
    else:
        rows = [u.model_dump() for u in users]

    if load_mode == "upsert":
        loaded = upsert_rows(dsn, target_table, rows, key_cols)
    elif load_mode == "copy":
        loaded = copy_dataframe(dsn, target_table, pd.DataFrame(rows))
    else:
        raise SystemExit(f"Unknown load mode: {load_mode!r}")

    metrics.loaded_count = loaded

    if source == "db":
        db_reader.set_watermark(db_reader.connect(dsn), "db", run_start)

    _log_final(metrics, source, load_mode, run_start)

def _log_final(metrics, source, load_mode, run_start):
    elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
    log.info("ETL run complete", extra={"source":source,"load_mode":load_mode,
             "elapsed_seconds":round(elapsed,2), **metrics.summary()})

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run ETL pipeline")
    ap.add_argument("--source", choices=["file","api","db"], required=True)
    ap.add_argument("--load", dest="load_mode", choices=["copy","upsert"], required=True)
    ap.add_argument("--incremental", action="store_true", default=False)
    args = ap.parse_args()
    run(args.source, args.load_mode, args.incremental)

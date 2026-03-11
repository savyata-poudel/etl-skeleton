from __future__ import annotations
import logging
from typing import Iterator
from pydantic import ValidationError
from etl.models import User

log = logging.getLogger(__name__)

class RunMetrics:
    def __init__(self):
        self.extracted_count: int = 0
        self.loaded_count: int = 0
        self.rejected_count: int = 0
    def summary(self) -> dict:
        return {"extracted_count": self.extracted_count,
                "loaded_count": self.loaded_count,
                "rejected_count": self.rejected_count}

def _missing_id(raw: dict) -> bool:
    uid = raw.get("id")
    return uid is None or str(uid).strip() == ""

def _empty_name(raw: dict) -> bool:
    return not str(raw.get("first_name", "")).strip()

def _reject(reason: str, raw: dict, metrics: RunMetrics) -> None:
    log.warning("Rejected row: %s | raw=%s", reason, raw)
    metrics.rejected_count += 1

def normalize_api_users(raw_iter: Iterator[dict], metrics: RunMetrics) -> Iterator[User]:
    for raw in raw_iter:
        metrics.extracted_count += 1
        if _missing_id(raw):
            _reject("missing id", raw, metrics); continue
        if _empty_name(raw):
            _reject("empty first_name", raw, metrics); continue
        try:
            yield User(id=int(raw["id"]), email=raw.get("email",""),
                       first_name=raw.get("first_name",""), last_name=raw.get("last_name",""),
                       avatar=raw.get("avatar"), source="api")
        except (ValidationError, ValueError) as exc:
            _reject(str(exc), raw, metrics)

def normalize_file_users(raw_iter: Iterator[dict], metrics: RunMetrics) -> Iterator[User]:
    for raw in raw_iter:
        metrics.extracted_count += 1
        if _missing_id(raw):
            _reject("missing id", raw, metrics); continue
        if _empty_name(raw):
            _reject("empty first_name", raw, metrics); continue
        try:
            is_active_raw = str(raw.get("is_active","true")).lower()
            yield User(id=int(str(raw["id"]).strip()), email=str(raw.get("email","")).strip(),
                       first_name=str(raw.get("first_name","")).strip(),
                       last_name=str(raw.get("last_name","")).strip(),
                       avatar=None, source="file",
                       department=str(raw.get("department","")).strip() or None,
                       hire_date=raw.get("hire_date") or None,
                       salary=float(raw["salary"]) if raw.get("salary") else None,
                       is_active=is_active_raw in ("true","1","yes"))
        except (ValidationError, ValueError) as exc:
            _reject(str(exc), raw, metrics)

def normalize_db_users(raw_iter: Iterator[dict], metrics: RunMetrics) -> Iterator[User]:
    for raw in raw_iter:
        metrics.extracted_count += 1
        if _missing_id(raw):
            _reject("missing id", raw, metrics); continue
        if _empty_name(raw):
            _reject("empty first_name", raw, metrics); continue
        try:
            yield User(id=int(raw["id"]), email=str(raw.get("email","")).strip(),
                       first_name=str(raw.get("first_name","")).strip(),
                       last_name=str(raw.get("last_name","")).strip(),
                       avatar=None, source="db",
                       department=str(raw.get("department","")).strip() or None,
                       hire_date=raw.get("hire_date"),
                       salary=float(raw["salary"]) if raw.get("salary") is not None else None,
                       is_active=bool(raw.get("is_active", True)))
        except (ValidationError, ValueError) as exc:
            _reject(str(exc), raw, metrics)

def normalize_customers(rows) -> Iterator[User]:
    metrics = RunMetrics()
    yield from normalize_api_users(iter(rows), metrics)

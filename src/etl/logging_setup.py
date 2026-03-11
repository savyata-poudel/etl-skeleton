from __future__ import annotations
import json, logging, logging.config, yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config" / "logging.yaml").exists():
            return parent
    return here.parents[3]

_ROOT = _find_root()

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        _skip = {"args","asctime","created","exc_info","exc_text","filename","funcName",
                 "id","levelname","levelno","lineno","module","msecs","message","msg",
                 "name","pathname","process","processName","relativeCreated","stack_info",
                 "thread","threadName","taskName"}
        for k, v in record.__dict__.items():
            if k not in _skip:
                log[k] = v
        return json.dumps(log, default=str)

def setup_logging() -> None:
    log_dir = _ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    path = _ROOT / "config" / "logging.yaml"
    with path.open() as f:
        cfg = yaml.safe_load(f)
    if "file" in cfg.get("handlers", {}):
        cfg["handlers"]["file"]["filename"] = str(log_dir / "etl.log")
    logging.config.dictConfig(cfg)

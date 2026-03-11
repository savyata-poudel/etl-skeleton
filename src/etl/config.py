from __future__ import annotations
import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config" / "settings.yaml").exists():
            return parent
    return here.parents[3]

_ROOT = _find_root()

def load_settings() -> dict:
    cfg_path = _ROOT / "config" / "settings.yaml"
    with cfg_path.open() as f:
        return yaml.safe_load(f)

def env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise ValueError(f"Environment variable ''{key}'' is not set. Copy .env.example to .env and fill in values.")
    return val

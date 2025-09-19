from __future__ import annotations
import os, yaml
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def load_settings() -> dict:
    cfg_path = Path(__file__).parent[2] / "config"/ "settings.yaml"
    with cfg_path.open() as f:
        return yaml.safe_load(f)   
    

def env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise ValueError(f"Environment variable {key} not set")
    return val
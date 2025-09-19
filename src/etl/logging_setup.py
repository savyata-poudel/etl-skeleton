import logging, logging.config, yaml
from pathlib import Path

def setup_logging() -> None:
    path = Path(__file__).parent.parent / "config" / "logging.yaml"
    with path.open() as f:
        cfg = yaml.safe_load(f)
    logging.config.dictConfig(cfg)
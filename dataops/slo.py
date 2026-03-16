from pathlib import Path
from typing import Any

import yaml


SLO_PATH = Path("slo.yml")


def load_slos() -> list[dict[str, Any]]:
    payload = yaml.safe_load(SLO_PATH.read_text(encoding="utf-8"))
    return payload.get("slos", [])


def slo_map() -> dict[str, dict[str, Any]]:
    return {item["name"]: item for item in load_slos()}

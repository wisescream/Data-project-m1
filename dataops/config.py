import os
from pathlib import Path
from typing import Any

import yaml


CONFIG_PATH = Path(os.getenv("LOGSTORM_CONFIG_PATH", "config.yml"))


def load_all_configs() -> dict[str, dict[str, Any]]:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def load_env_config(env: str | None = None) -> dict[str, Any]:
    env_name = env or os.getenv("ENV", "dev")
    configs = load_all_configs()
    if env_name not in configs:
        raise KeyError(f"Unknown LogStorm environment '{env_name}' in {CONFIG_PATH}")
    return {"name": env_name, **configs[env_name]}


def storage_uri(env_config: dict[str, Any], key: str, spark: bool = False) -> str:
    raw = str(env_config[key])
    if spark and raw.startswith("minio://"):
        return "s3a://" + raw.removeprefix("minio://")
    return raw

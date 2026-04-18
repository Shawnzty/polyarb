from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "scan": {
        "within_hours": 24.0,
        "target_sizes": [100.0, 500.0, 1000.0],
    },
    "risk": {
        "leg_risk_bps_per_extra_leg": 25.0,
        "other_outcome_penalty_bps": 100.0,
        "augmented_neg_risk_penalty_bps": 50.0,
    },
}


def load_config(path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    config = deepcopy(DEFAULT_CONFIG)
    config_path = Path(path) if path else Path("config.yaml")
    if config_path.exists():
        merge_config(config, parse_simple_yaml(config_path))
    return config


def merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_config(base[key], value)
        else:
            base[key] = value
    return base


def parse_simple_yaml(path: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    current_section: Optional[str] = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if not line.startswith(" ") and line.endswith(":"):
            current_section = line[:-1].strip()
            result.setdefault(current_section, {})
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        parsed_value = parse_scalar(value.strip())
        if current_section and raw_line.startswith(" "):
            section = result.setdefault(current_section, {})
            if isinstance(section, dict):
                section[key] = parsed_value
        else:
            result[key] = parsed_value
            current_section = None
    return result


def parse_scalar(value: str) -> Any:
    if value == "":
        return {}
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"none", "null"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        parsed = ast.literal_eval(value)
        return [float(item) if isinstance(item, (int, float)) else item for item in parsed]
    try:
        number = float(value)
    except ValueError:
        return value.strip("\"'")
    return int(number) if number.is_integer() else number

####################################
# Utility functions for Tradeflows #
####################################

# Purpose: houses useful functions for project Tradeflows
# Mainly functions for validating JSONs, dates, timestamps, etc

from collections.abc import Iterable
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import asdict, is_dataclass


def _json_ready(obj):
    """
    JSON only supports dict/list/str/number/bool/null.
    Converts unfriedly object to JSON-friendly formats.
    Note, this is recursive to handle dictetc.
    """
    if isinstance(obj, dict):
        return {k: _json_ready(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_ready(v) for v in obj]
    if isinstance(obj, tuple):
        return [_json_ready(v) for v in obj]
    if isinstance(obj, set):
        return [_json_ready(v) for v in sorted(obj)]
    if isinstance(obj, Path):
        return str(obj)
    return obj


def _dict_ready(obj):

    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, tuple):
        return list(obj)
    if isinstance(obj, dict):
        return {str(k): _dict_ready(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dict_ready(x) for x in obj]
    return obj


def has_any_txt(dirpath: Path) -> bool:
    """
    True if directory contains at least one .txt file (recursively).
    """
    return any(p.suffix.lower() == ".txt" for p in dirpath.glob("**/*"))


def has_any_parquet(dirpath: Path) -> bool:
    return any(p.is_file() for p in dirpath.rglob("*.parquet"))


def as_int_list(x) -> list[int]:
    """
    Accepts a string of country codes (e.g. "840, 841, 842") and returns a list of integers (e.g. [840, 841, 842])
    """
    if x is None:
        return []

    if isinstance(x, str):
        return [int(v.strip()) for v in x.split(",") if v.strip()]

    if isinstance(x, int):
        return [x]

    if isinstance(x, Iterable) and not isinstance(x, str):
        return [int(v) for v in x]

    raise TypeError(f"Cannot convert {type(x)} to list[int].")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

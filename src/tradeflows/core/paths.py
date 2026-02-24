####################################
# Util functions for setting paths #
####################################
# By: DDR

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    base: Path
    raw: Path
    parquet: Path
    logs: Path
    meta: Path


def project_root() -> Path:
    # file path
    here = Path(__file__).resolve()

    for candidate in [here.parent, *here.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate

    raise RuntimeError("Could not find project root (no pyproject.toml found).")


def dataset_paths(dataset: str) -> DataPaths:
    """
    Returns DataPaths for a given dataset ('comtrade' or 'census')
    """
    base = project_root() / "data" / dataset
    raw = base / "raw"
    parquet = base / "parquet"
    logs = base / "logs"
    meta = base / "meta"
    return DataPaths(base=base, raw=raw, parquet=parquet, logs=logs, meta=meta)


def ensure_dataset_dirs(dataset: str) -> DataPaths:
    """
    Creates, or verifies existence, of paths for a given dataset ('comtrade' or 'census')
    """
    paths = dataset_paths(dataset)
    for p in [paths.base, paths.raw, paths.parquet, paths.logs, paths.meta]:
        p.mkdir(parents=True, exist_ok=True)
    return paths

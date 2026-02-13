######################
# TRADEFLOWS PROJECT #
######################
# Author: DDR
# Purpose: Bulk download, foldering, create parquets

# prepare workspace
from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
import os
import re
import inspect
import logging
import json
import pandas as pd
from dotenv import load_dotenv
from collections.abc import Iterable
from datetime import datetime, timezone
import comtradeapicall

# import own modules
from tradeflows_cli.paths import ensure_dataset_dirs, DataPaths
from tradeflows_cli.logging import setup_logging, get_logger
from tradeflows_cli.hs_codes import hs_index_path, update_hs_index

logger = get_logger() # module-level logger

### 1. Configure  -------------------------------------

def _json_ready(obj):
    """
    JSON only supports dict/list/str/number/bool/null.
    Converts dicts/tuples to lists so JSON can handle them.
    Note, this is recursive to handle dicts within dicts, etc.
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

def write_manifest(report: MultiDownloadReport, manifest_path: Path) -> Path:
    """
    Convert MultiDownloadReport and content (including DownloadReportperReporter) into JSON manifest
    """
    payload = _json_ready(asdict(report))
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    # write the JSON
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


_ALLOWED_DATASETS = {"Tariffline", "Final"}
_ALLOWED_FREQ = {"M", "A"}


@dataclass(frozen=True)
class ComtradeBulkConfig:
    """
    Config file for a data retrieval fetch.
    """
    reporterCodes: tuple[int,...]   # tuple of integer codes
    start: str                      # "YYYY-mm" if monthly, "YYYY" if annual
    end: str
    typeCode: str = "C"             # commodities, can be "S" for services
    clCode: str = "HS"              # HS classification system
    freqCode: str = "M"             # "M" is monthly, "A" is annual
    dataset: str = "Tariffline"     # for raw/reported data "Tariffline"; for harmonised data "Final"
    decompress: bool = True
    overwrite: bool = False

    def __post_init__(self) -> None:

        # dataset
        if self.dataset not in _ALLOWED_DATASETS:
            raise ValueError(f"dataset must be one of {_ALLOWED_DATASETS}, got {self.dataset}")

        # freq
        if self.freqCode not in _ALLOWED_FREQ:
            raise ValueError(f"freqCode must be one of {_ALLOWED_FREQ}, got {self.freqCode}")
        
        # reporter codes
        if not self.reporterCodes:
            raise ValueError("reporterCodes cannot be empty")
        if any((not isinstance(x, int)) for x in self.reporterCodes):
            raise TypeError("reporterCodes must be ints")
        if any(x <= 0 for x in self.reporterCodes):
            raise ValueError("reporterCodes must be positive")

        # date formats
        if self.freqCode == "M":
            if not re.fullmatch(r"\d{4}-\d{2}", self.start):
                raise ValueError("Monthly start must be 'YYYY-MM'")
            if not re.fullmatch(r"\d{4}-\d{2}", self.end):
                raise ValueError("Monthly end must be 'YYYY-MM'")
        else:  # "A"
            if not re.fullmatch(r"\d{4}", self.start):
                raise ValueError("Annual start must be 'YYYY'")
            if not re.fullmatch(r"\d{4}", self.end):
                raise ValueError("Annual end must be 'YYYY'")

        # optional: logical ordering check
        if self.start > self.end:
            raise ValueError(f"start must be <= end, got start='{self.start}' end='{self.end}'")



### 2. Helper functions --------------------------------


# i. List helpers

def build_period_list(freqCode: str, start: str, end: str) -> list[str]:
    """
    A function which returns a list of periods from start to end, either monthly or annual.

    :param freqCode: "M" or "A" for monthly or annual
    :type freqCode: str
    :param start: start of the list
    :type start: str
    :param end: end of the list
    :type end: str
    :return: a list of valid periods from start to end, e.g ["1991","1992",...]
    :rtype: list[str]
    """
    #  monthly format checks
    if freqCode == "M":
        # monthly range
        pr = pd.period_range(start=pd.Period(start, "M"), end=pd.Period(end, "M"))
        return [f"{p.year}{p.month:02d}" for p in pr]
    # annual format checks
    elif freqCode == "A":
        # yearly range
        pr = range(int(start), int(end) + 1)
        return [str(p) for p in pr]
    
    else: raise ValueError("Invalid frequency code.")

# ii. directory helpers

def raw_period_dir(
    paths: DataPaths, 
    cfg: ComtradeBulkConfig, 
    period: str, 
    reporter: int,
) -> Path:
    """
    Creates a folder for raw data for a given reporter, period pair.  Consistent with Hive partitioning.
    """
    p = (
        paths.raw
        / f"dataset={cfg.dataset}"
        / f"type={cfg.typeCode}"
        / f"cl={cfg.clCode}"
        / f"freq={cfg.freqCode}"
        / f"reporter={reporter}"
        / f"period={period}"
    )
    p.mkdir(parents=True, exist_ok=True)
    return p

def parquet_period_dir(
    paths: DataPaths, 
    cfg: ComtradeBulkConfig, 
    period: str,
    reporter: int,
) -> Path:
    """
    Creates a folder for paqruet data for a given reporter, period pair.  Consistent with Hive partitioning.
    """
    p = (
        paths.parquet
        / f"dataset={cfg.dataset}"
        / f"type={cfg.typeCode}"
        / f"cl={cfg.clCode}"
        / f"freq={cfg.freqCode}"
        / f"reporter={reporter}"
        / f"period={period}"
    )
    p.mkdir(parents=True, exist_ok=True)
    return p


def has_any_txt(dirpath: Path) -> bool:
    """
    True if directory contains at least one .txt file (recursively).
    """
    return any(p.suffix.lower() == ".txt" for p in dirpath.glob("**/*"))

def has_any_parquet(dirpath: Path) -> bool:
    return any(p.is_file() for p in dirpath.rglob("*.parquet"))



# iii. parquet writer

def convert_period_txt_to_parquet(
    raw_dir: Path,
    parquet_dir: Path,
    *,
    overwrite: bool = False,
    sep: str = "\t",
    chunk_size: int | None = None,
) -> tuple[list[Path], set[str]]:
    """
    Converts every .txt file in raw_dir into a .parquet file in parquet_dir.
    Returns a list of parquets created.
    """
    txt_files = sorted(raw_dir.glob("**/*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found under {raw_dir}!")

    written: list[Path] = []
    per_period_cmdCodes = set() # catcher of unique codes


    for txt in txt_files:

        output_file = parquet_dir / (txt.stem + ".parquet")
        
        if output_file.exists() and (not overwrite):
            logger.debug("  Skipping Parquet conversion...parquet already exists")
            continue

        if chunk_size is None:
            df = pd.read_csv(txt, sep=sep, low_memory=False)
            per_period_cmdCodes.update(df["cmdCode"].dropna().astype(str).str.strip()) # extract unique HS codes
            df.to_parquet(output_file, index=False)
            written.append(output_file)
        else:
            part = 0
            for chunk in pd.read_csv(
                txt, sep=sep, low_memory=False, chunksize=chunk_size
            ):
                part_file = parquet_dir / f"{txt.stem}.part{part:04d}.parquet"
                if part_file.exists() and (not overwrite):
                    logger.debug("  Skipping Parquet part %s conversion...parquet already exists", part)
                    part += 1
                    continue
                else:
                    per_period_cmdCodes.update(chunk["cmdCode"].dropna().astype(str).str.strip()) # extract unique HS codes
                    chunk.to_parquet(part_file, index=False)
                    written.append(part_file)
                    part += 1

    return written, per_period_cmdCodes

# iii. Checking data availability in UN comtrade

def require_fn(name: str):
    fn = getattr(comtradeapicall, name, None)
    if fn is None:
        available = [n for n, o in inspect.getmembers(comtradeapicall, inspect.isfunction)]
        raise RuntimeError(
            f"Your comtradeapicall install has no '{name}'.\n"
            f"Available functions include: {available}"
        )
    return fn


def get_bulk_availability(api_key: str, *, reporterCode: int, cfg: ComtradeBulkConfig) -> pd.DataFrame | None:
    """
    Returns availability dataframe which must have a 'period' column.
    Tariffline availability if cfg.dataset='tariffline'. Final availability if cfg.dataset='Final'.
    """
    if cfg.dataset == "Tariffline":
        fn = require_fn("getTarifflineDataBulkAvailability")  
        df = fn(
            api_key,
            typeCode=cfg.typeCode,
            freqCode=cfg.freqCode,
            clCode=cfg.clCode,
            period=None,
            reporterCode=reporterCode,
        )
    elif cfg.dataset == "Final":
        fn = require_fn("getFinalDataBulkAvailability")       
        df = fn(
            api_key,
            typeCode=cfg.typeCode,
            freqCode=cfg.freqCode,
            clCode=cfg.clCode,
            period=None,
            reporterCode=reporterCode,
        )
    else:
        raise ValueError("Please insert a valid comtrade dataset ('Tariffline' or 'Final')") 

    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    
    if df.empty:
        return None
    else:
        df["period"] = df["period"].astype(str)
        return df


def select_available_periods(
    requested_periods: list[str], availability_df: pd.DataFrame
) -> list[str]:
    """
    Ensures periods in requested_periods are available as per availability_df.
    """
    if "period" not in availability_df.columns:
        raise ValueError("availability_df must have a column called 'period'")
    available = set(availability_df["period"])
    return sorted([p for p in requested_periods if p in available])

# iv. Comtrade download functions

def download_one_period(
        api_key: str, 
        cfg : ComtradeBulkConfig, 
        paths: DataPaths,
        reporter: int,
        period: str, 
        *,
        convert_to_parquet: bool = True, 
        chunk_size: int | None = 1_000_000,
) -> set[str]:
    """
    Download one raw .txt file per reporter, period pair.
    """
    raw_dir = raw_period_dir(paths, cfg, period, reporter)

    # if cfg.overwrite is False and there is a raw file already
    if (not cfg.overwrite) and has_any_txt(raw_dir):
        logger.info("Skipping download...raw data for reporter %s in %s already exists.", reporter, period)
    else: 
        logger.info("Downloading %s data for reporter %s in %s.", cfg.dataset, reporter, period) 

        if cfg.dataset=="Tariffline":
            fn = require_fn("bulkDownloadTarifflineFile")
        else:
            fn = require_fn("bulkDownloadFinalFile")

        # download data
        try:
            fn(
                api_key,
                directory=str(raw_dir),
                typeCode=cfg.typeCode,
                freqCode=cfg.freqCode,
                clCode=cfg.clCode,
                period=period,
                reporterCode=reporter,
                decompress=cfg.decompress,
            )
        except Exception:
            logger.exception(
                "Download failed dataset=%s reporter=%s period=%s raw_dir=%s",
                cfg.dataset, reporter, period, raw_dir
            )
            raise

    per_period_cmdCodes: set[str] = set()

    if convert_to_parquet:
        pq_dir = parquet_period_dir(paths, cfg, period, reporter)
        written, per_period_cmdCodes = convert_period_txt_to_parquet(raw_dir, pq_dir, overwrite=cfg.overwrite, chunk_size=chunk_size)
        idx = hs_index_path(paths.meta, dataset=cfg.dataset, clCode=cfg.clCode, freqCode=cfg.freqCode)
        _ = update_hs_index(idx.index_file, new_codes=per_period_cmdCodes)

        if written:
            logger.info(" Parquet written: %s file(s) for %s.", len(written), period)

    return per_period_cmdCodes
        

@dataclass(frozen=True)
class DownloadReportperReporter:
    """
    Download report per reporter.
    """
    reporter: int
    requested: list[str]
    available_within_request: list[str]
    missing_within_request: list[str]
    downloaded_or_present: list[str]
    min_available: str | None
    max_available: str | None
    unique_hs_codes: set[str]


def download_bulk_range_one(
        api_key: str,
        cfg: ComtradeBulkConfig,
        paths: DataPaths,
        *,
        reporter: int, 
    ) -> DownloadReportperReporter:
    """
    For one reporter, download requested dataset from cfg.start to cfg.end.
    """
    requested_periods = build_period_list(cfg.freqCode, cfg.start, cfg.end)
    
    available_df = get_bulk_availability(api_key, reporterCode=reporter, cfg=cfg)

    per_reporter_cmdCodes: set[str] = set()

    if available_df is None: 
        logger.info("No available %s data at all for reporter %s.", cfg.dataset, reporter)
        return DownloadReportperReporter(
            reporter=reporter, 
            requested=requested_periods,
            available_within_request=[],
            missing_within_request=requested_periods,
            downloaded_or_present=[],
            min_available=None,
            max_available=None,
            unique_hs_codes=set(),
        )
    
    available_periods = select_available_periods(requested_periods, available_df)
    available_set = set(available_periods)
    missing = [p for p in requested_periods if p not in available_set]

    min_possible = str(available_df["period"].min()) if not available_df.empty else None
    max_possible = str(available_df["period"].max()) if not available_df.empty else None

    downloaded_or_present: list[str] = []
    
    for period in available_periods:
        per_period_cmdCodes = download_one_period(api_key, cfg, paths, reporter = reporter, period=period)
        downloaded_or_present.append(period)
        per_reporter_cmdCodes.update(per_period_cmdCodes)

    return DownloadReportperReporter(
        reporter=reporter,
        requested=requested_periods,
        available_within_request=available_periods,
        missing_within_request=missing,
        downloaded_or_present=downloaded_or_present,
        min_available=min_possible,
        max_available=max_possible,
        unique_hs_codes=per_reporter_cmdCodes,
    )


@dataclass(frozen=True)
class MultiDownloadReport:
    dataset: str
    freqCode: str
    start: str
    end: str
    reporters: tuple[int, ...]
    created_utc: str
    # totals
    total_reporters: int
    total_requested_periods: int
    total_downloaded_periods: int
    total_missing_periods: int
    # detail
    per_reporter: dict[int, DownloadReportperReporter]


def download_bulk_range_many(
        api_key: str, 
        cfg: ComtradeBulkConfig,
        paths: DataPaths
    ) -> MultiDownloadReport:

    per: dict[int, DownloadReportperReporter] = {}

    for reporter in cfg.reporterCodes:
        report_per_reporter = download_bulk_range_one(api_key, cfg, paths, reporter=reporter)
        per[reporter] = report_per_reporter

    # totals (simple and transparent)
    total_requested_periods = sum(len(r.requested) for r in per.values())
    total_downloaded_periods = sum(len(r.downloaded_or_present) for r in per.values())
    created_utc = datetime.now(timezone.utc).isoformat()

    return MultiDownloadReport(
        dataset=cfg.dataset,
        freqCode=cfg.freqCode,
        start=cfg.start,
        end=cfg.end,
        reporters=cfg.reporterCodes,
        created_utc=created_utc,
        total_reporters=len(per),
        total_requested_periods=total_requested_periods,
        total_downloaded_periods=total_downloaded_periods,
        total_missing_periods=total_requested_periods-total_downloaded_periods,
        per_reporter=per,
    )


# v. Util helpers

def as_int_list(x) -> list[int]:
    """
    Accepts a string of country codes (e.g. "840, 841, 842") and returns a list of integers (e.g. [840, 841, 842])
    """
    if x is None: return []

    if isinstance(x, str):
        return [int(v.strip()) for v in x.split(",") if v.strip()]

    if isinstance(x, int): return [x]

    if isinstance(x, Iterable) and not isinstance(x, str):
        return [int(v) for v in x]

    raise TypeError(f"Cannot convert {type(x)} to list[int].")


## 3. Execution code ----------------

def run_comtrade_download(
    *,
    iso3_codes: list[str],
    start: str,
    end: str,
    dataset: str = "Tariffline",
    freqCode: str = "M",
    typeCode: str = "C",
    clCode: str = "HS",
    decompress: bool = True,
    overwrite: bool = False,
) -> None:
    
    load_dotenv()
    api_key = os.getenv("COMTRADE_API_KEY_PRIMARY")
    if not api_key:
        raise RuntimeError("Missing COMTRADE_API_KEY_PRIMARY in .env")

    paths = ensure_dataset_dirs("comtrade")
    _ = setup_logging(paths.logs, timestamped=True)

    mapping: dict[str, list[int]] = {}
    for iso3 in iso3_codes:
        raw = comtradeapicall.convertCountryIso3ToCode(iso3)
        mapping[iso3] = as_int_list(raw)

    all_reporter_codes = sorted(
        {code for codes in mapping.values() for code in codes}
    )

    cfg = ComtradeBulkConfig(
        reporterCodes=tuple(all_reporter_codes),
        start=start,
        end=end,
        typeCode=typeCode,
        clCode=clCode,
        freqCode=freqCode,
        dataset=dataset, 
        decompress=decompress, 
        overwrite=overwrite,
    )

    multi = download_bulk_range_many(api_key, cfg, paths)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = paths.logs / f"download_manifest_{ts}.json"
    write_manifest(multi, manifest_path)
    logger.info("Manifest written: %s", manifest_path)








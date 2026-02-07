######################
# TRADEFLOWS PROJECT #
######################
# Author: DDR
# Prupose: Bulk download, foldering, create parquets

# prepare workspace
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os
import re
import pandas as pd
from dotenv import load_dotenv
import comtradeapicall


### 1. Configure  -------------------------------------


@dataclass(frozen=True)
class ComtradeBulkConfig:
    reporterCode: int  # integer code, will set
    start: str  # "YYYY-mm" if monthly, "YYYY" if annual
    end: str
    typeCode: str = "C"  # commodities, can be "S" for services
    clCode: str = "HS"  # HS classification system
    freqCode: str = "M"  # "M" is monthly, "A" is annual
    decompress: bool = True
    overwrite: bool = False


### 2. Helper functions --------------------------------


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
        if not re.fullmatch(r"\d{4}-\d{2}", start):
            raise ValueError("Monthly start must be 'YYYY-MM'")
        if not re.fullmatch(r"\d{4}-\d{2}", end):
            raise ValueError("Monthly end must be 'YYYY-MM'")
        # monthly range
        pr = pd.period_range(start=pd.Period(start, "M"), end=pd.Period(end, "M"))
        return [f"{p.year}{p.month:02d}" for p in pr]
    # annual format checks
    elif freqCode == "A":
        if not re.fullmatch(r"\d{4}", start):
            raise ValueError("Annual start must be 'YYYY'")
        if not re.fullmatch(r"\d{4}", end):
            raise ValueError("Annual end must be 'YYYY'")
        # yearly range
        pr = range(int(start), int(end) + 1)
        return [str(p) for p in pr]
    # invalid freqCode
    else:
        raise ValueError("Please insert valid freqCode ('M' or 'A')")


def get_bulk_availability(
    api_key: str,
    reporterCode: int,
    freqCode: str,
    typeCode: str = "C",
    clCode: str = "HS",
) -> pd.DataFrame:
    """
    Returns pd df of availability using comtradeapicall.getFinalDataBulkAvailability().
    """
    df = comtradeapicall.getFinalDataBulkAvailability(
        api_key,
        typeCode=typeCode,
        freqCode=freqCode,
        clCode=clCode,
        period=None,
        reporterCode=reporterCode,
    )
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
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


def ensure_base_dirs(project_root: Path) -> dict[str, Path]:
    """
    Ensures correct base directories are created or exist.
    """
    base = project_root / "data" / "comtrade"
    paths = {
        "base": base,
        "raw": base / "raw",
        "parquet": base / "parquet",
        "logs": base / "logs",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


def raw_period_dir(
    paths: dict[str, Path], cfg: ComtradeBulkConfig, period: str
) -> Path:
    """
    Creates a series of folders consistent with Hive partitioning for the raw data (.txt files).
    """
    p = (
        paths["raw"]
        / f"type={cfg.typeCode}"
        / f"cl={cfg.clCode}"
        / f"freq={cfg.freqCode}"
        / f"reporter={cfg.reporterCode}"
        / f"period={period}"
    )
    p.mkdir(parents=True, exist_ok=True)
    return p


def parquet_period_dir(
    paths: dict[str, Path], cfg: ComtradeBulkConfig, period: str
) -> Path:
    """
    Creates a series of folders consistent with Hive partitioning for parquets.
    """
    p = (
        paths["parquet"]
        / f"type={cfg.typeCode}"
        / f"cl={cfg.clCode}"
        / f"freq={cfg.freqCode}"
        / f"reporter={cfg.reporterCode}"
        / f"period={period}"
    )
    p.mkdir(parents=True, exist_ok=True)
    return p


def has_any_txt(dirpath: Path) -> bool:
    """
    True if directory contains at least one .txt file (recursively).
    """
    return any(p.suffix.lower() == ".txt" for p in dirpath.glob("**/*"))


def convert_period_txt_to_parquet(
    raw_dir: Path,
    parquet_dir: Path,
    *,
    overwrite: bool = False,
    sep: str = "\t",
    chunk_size: int | None = None,
) -> list[Path]:
    """
    Converts every .txt file in raw_dir into a .parquet file in parquet_dir.
    Returns a list of parquets created.
    """
    txt_files = sorted(raw_dir.glob("**/*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found under {raw_dir}!")

    written: list[Path] = []

    for txt in txt_files:
        output_file = parquet_dir / (txt.stem + ".parquet")

        if output_file.exists() and (not overwrite):
            continue

        if chunk_size is None:
            df = pd.read_csv(txt, sep=sep, low_memory=False)
            df.to_parquet(output_file, index=False)
            written.append(output_file)
        else:
            part = 0
            for chunk in pd.read_csv(
                txt, sep=sep, low_memory=False, chunksize=chunk_size
            ):
                part_file = parquet_dir / f"{txt.stem}.part{part:04d}.parquet"
                if part_file.exists() and (not overwrite):
                    continue
            chunk.to_parquet(part_file, index=False)
            written.append(part_file)
            part += 1

    return written


def download_one_period(
    api_key: str,
    cfg: ComtradeBulkConfig,
    paths: dict[str, Path],
    period: str,
    *,
    convert_to_parquet: bool = True,
    chunk_size: int | None = None,
) -> None:
    """
    Download bulk comtrade file into existing directory for a given cfg. Convert to parquet file.
    """
    raw_dir = raw_period_dir(paths, cfg, period)
    if (not cfg.overwrite) and has_any_txt(raw_dir):
        print(f"Skip {period} (already downloaded for reporter {cfg.reporterCode})")

    else:
        print(f"Downloading {period} for reporter {cfg.reporterCode}...")
        comtradeapicall.bulkDownloadFinalFile(
            api_key,
            directory=str(raw_dir),
            typeCode=cfg.typeCode,
            freqCode=cfg.freqCode,
            clCode=cfg.clCode,
            period=period,
            reporterCode=cfg.reporterCode,
            decompress=cfg.decompress,
        )

    if convert_to_parquet:
        parquet_dir = parquet_period_dir(paths, cfg, period)
        written = convert_period_txt_to_parquet(
            raw_dir,
            parquet_dir,
            overwrite=cfg.overwrite,
            sep="\t",
            chunk_size=chunk_size,
        )
        if written:
            print(f" Parquet written: {len(written)} file(s) for {period}")


# Create a class to report back a request
@dataclass(frozen=True)
class DownloadReport:
    requested: list[str]
    available_within_request: list[str]
    missing_within_request: list[str]
    downloaded_or_present: list[str]
    min_available: str | None
    max_available: str | None


def download_bulk_range(
    api_key: str, cfg: ComtradeBulkConfig, paths: dict[str, Path]
) -> DownloadReport:
    """
    For a given cgf and base paths, downloads bulk Comtrafe data.
    """

    requested_periods = build_period_list(cfg.freqCode, cfg.start, cfg.end)

    available_df = get_bulk_availability(
        api_key=api_key,
        reporterCode=cfg.reporterCode,
        freqCode=cfg.freqCode,
        typeCode=cfg.typeCode,
        clCode=cfg.clCode,
    )

    available_periods = select_available_periods(requested_periods, available_df)
    available_set = set(available_periods)

    missing = [p for p in requested_periods if p not in available_set]

    min_possible = str(available_df["period"].min())
    max_possible = str(available_df["period"].max())

    downloaded_or_present = []
    for period in available_periods:
        download_one_period(
            api_key, cfg, paths, period, convert_to_parquet=True, chunk_size=1_000_000
        )
        downloaded_or_present.append(period)

    return DownloadReport(
        requested=requested_periods,
        available_within_request=available_periods,
        missing_within_request=missing,
        downloaded_or_present=downloaded_or_present,
        min_available=min_possible,
        max_available=max_possible,
    )


## 3. Example code ----------------

load_dotenv()
api_key = os.getenv("COMTRADE_API_KEY_PRIMARY")
if not api_key:
    raise RuntimeError("Missing COMTRADE_API_KEY_PRIMARY in .env")

project_root = Path(__file__).resolve().parent
paths = ensure_base_dirs(project_root)

cfg = ComtradeBulkConfig(
    reporterCode=826,
    start="2010-01",
    end="2010-04",
    freqCode="M",
)

report = download_bulk_range(api_key, cfg, paths)

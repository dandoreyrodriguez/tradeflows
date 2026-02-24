######################
# TRADEFLOWS PROJECT #
######################
# Author: DDR
# Purpose: Bulk download, foldering, create parquets

# prepare workspace
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os
import re
import inspect
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone
import comtradeapicall

# import own modules
from tradeflows.core.paths import ensure_dataset_dirs, DataPaths
from tradeflows.core.logging import setup_logging, get_logger
from tradeflows.core.utils import has_any_txt, as_int_list, _utc_now_iso, _dict_ready
from tradeflows.metadata.hs_codes import hs_index_path, update_hs_index

logger = get_logger()  # module-level logger
# allowed Comtrade arguments
_ALLOWED_DATASETS = {"Tariffline", "Final"}
_ALLOWED_FREQ = {"M", "A"}

### 1. Comtrade Functions and classes -------------------------------------


@dataclass(frozen=True)
class ComtradeBulkConfig:
    """
    A dataclass for configuring Bulk Comtrade download calls.
    """

    reporterCodes: tuple[int, ...]  # tuple of integer codes
    start: str  # "YYYY-mm" if monthly, "YYYY" if annual
    end: str
    typeCode: str = "C"  # commodities, can be "S" for services
    clCode: str = "HS"  # HS classification system
    freqCode: str = "M"  # "M" is monthly, "A" is annual
    dataset: str = (
        "Tariffline"  # for raw/reported data "Tariffline"; for harmonised data "Final"
    )
    decompress: bool = True
    overwrite: bool = False  # overwrites existing data

    def __post_init__(self) -> None:

        # dataset
        if self.dataset not in _ALLOWED_DATASETS:
            raise ValueError(
                f"dataset must be one of {_ALLOWED_DATASETS}, got {self.dataset}"
            )

        # freq
        if self.freqCode not in _ALLOWED_FREQ:
            raise ValueError(
                f"freqCode must be one of {_ALLOWED_FREQ}, got {self.freqCode}"
            )

        # reporter codes
        if not self.reporterCodes:
            raise ValueError("reporterCodes cannot be empty")
        if any((not isinstance(x, int)) for x in self.reporterCodes):
            raise TypeError("All reporterCodes must be integers.")
        if any(x <= 0 for x in self.reporterCodes):
            raise ValueError("reporterCodes must be positive integers.")

        # date formats
        # monthly
        if self.freqCode == "M":
            if not re.fullmatch(r"\d{4}-\d{2}", self.start):
                raise ValueError("Monthly start must be 'YYYY-MM'")
            if not re.fullmatch(r"\d{4}-\d{2}", self.end):
                raise ValueError("Monthly end must be 'YYYY-MM'")
        # annual
        else:
            if not re.fullmatch(r"\d{4}", self.start):
                raise ValueError("Annual start must be 'YYYY'")
            if not re.fullmatch(r"\d{4}", self.end):
                raise ValueError("Annual end must be 'YYYY'")

        # ordering check
        if self.start > self.end:
            raise ValueError(
                f"start must be <= end, got start='{self.start}' end='{self.end}'"
            )


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

    else:
        raise ValueError("Invalid frequency code.")


def raw_period_reporter_dir(
    paths: DataPaths,
    cfg: ComtradeBulkConfig,
    period: str,
    reporter: int,
) -> Path:
    """
    Creates a folder for the raw data for a given reporter, period pair. Consistent with Hive partitioning.
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
    Creates a folder for parquet data for a given reporter, period pair. Consistent with Hive partitioning.
    This will house what is in `raw` once converted.
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
    If the .txt is large enough and a chunk_size is specified it will do this
    by creating several parquets.

    Returns a list of parquets created.
    """

    txt_files = sorted(raw_dir.glob("**/*.txt"))  # all .txt files in raw_dir
    if not txt_files:
        raise ValueError(f"No .txt files found under {raw_dir}!")

    written: list[Path] = []  # to catch new parquets
    per_period_cmdCodes = set()  # catcher of unique codes

    # loop over txt_files
    for txt in txt_files:

        processed_ok = False  # to keep track of progress

        try:

            output_file = parquet_dir / "raw.parquet"  # file of parquet-to-be
            if output_file.exists() and (not overwrite):
                logger.debug("  Skipping Parquet conversion...parquet already exists")
                continue

            # if chunk_size is None write one parquet
            if chunk_size is None:
                df = pd.read_csv(txt, sep=sep, low_memory=False)
                per_period_cmdCodes.update(
                    df["cmdCode"].dropna().astype(str).str.strip()
                )  # extract unique HS codes
                df.to_parquet(
                    output_file, index=False, compression="zstd"
                )  # compresses data
                written.append(output_file)
            # else if chunk_size is specified write parquets in parts
            else:
                part = 0  # start with part 0
                for chunk in pd.read_csv(
                    txt, sep=sep, low_memory=False, chunksize=chunk_size
                ):
                    part_file = parquet_dir / f"raw.part{part:04d}.parquet"
                    if part_file.exists() and (not overwrite):
                        logger.debug(
                            "  Skipping Parquet part %s conversion...parquet already exists",
                            part,
                        )
                        part += 1
                        continue
                    else:
                        per_period_cmdCodes.update(
                            chunk["cmdCode"].dropna().astype(str).str.strip()
                        )  # extract unique HS codes
                        chunk.to_parquet(part_file, index=False, compression="zstd")
                        written.append(part_file)
                        part += 1

            processed_ok = True  # writing has been successful

        # unlink text file if parquet file creation successful (for memory)
        finally:
            if processed_ok:
                try:
                    txt.unlink(missing_ok=True)
                except Exception:
                    logger.exception("Failed to delete raw .txt : %s", txt)

    return written, per_period_cmdCodes


def require_fn(name: str):
    """
    Returns attribute of comtradeapicall which matches name supplied. If not, returns valid attributes.

    :param name: function name (e.g. "getTarifflineDataBulkAvailability")
    :type name: str
    """
    fn = getattr(comtradeapicall, name, None)  # gets the attribute
    if fn is None:
        available = [
            n for n, o in inspect.getmembers(comtradeapicall, inspect.isfunction)
        ]
        raise RuntimeError(
            f"Your comtradeapicall install has no '{name}'.\n"
            f"Available functions include: {available}"
        )
    return fn


def get_bulk_availability(
    api_key: str, *, reporterCode: int, cfg: ComtradeBulkConfig
) -> pd.DataFrame | None:
    """
    Returns availability dataframe for a given reporterCode which must have a 'period' column.
    Remember, it is specific to the type of data called (i.e. "Tariffline" or "Final")
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
        raise ValueError(
            "Please insert a valid comtrade dataset ('Tariffline' or 'Final')"
        )

    # convert to df if not
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    # if empty return None (makes diagnoising easier down the line)
    if df.empty:
        return None

    # returns the list of periods available
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
    # return a sorter list of months which are available
    return sorted([p for p in requested_periods if p in available])


def download_one_period(
    api_key: str,
    cfg: ComtradeBulkConfig,
    paths: DataPaths,
    reporter: int,
    period: str,
    *,
    convert_to_parquet: bool = True,
    chunk_size: int | None = 1_000_000,
) -> set[str]:
    """
    Download sone raw .txt file per reporter, period pair and store in a names path in paths.
    Converts to parquet if convert_to_parquet is True.
    Returns unique HS codes which are retrieved.
    """

    raw_dir = raw_period_reporter_dir(
        paths, cfg, period, reporter
    )  # sets up right folder foor period, reporter

    # if cfg.overwrite is False and there is a raw file already
    if (not cfg.overwrite) and has_any_txt(raw_dir):
        logger.info(
            "Skipping download...raw data for reporter %s in %s already exists.",
            reporter,
            period,
        )
    else:
        logger.info(
            "Downloading %s data for reporter %s in %s.", cfg.dataset, reporter, period
        )

        # set up right datset
        if cfg.dataset == "Tariffline":
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
                cfg.dataset,
                reporter,
                period,
                raw_dir,
            )
            raise

    per_period_reporter_cmdCodes: set[str] = set()  # catcher for unique hs codes

    # write parquets (and deletes .txt files in convert_period_txt_to_parquet)
    if convert_to_parquet:
        pq_dir = parquet_period_dir(paths, cfg, period, reporter)
        written, per_period_reporter_cmdCodes = convert_period_txt_to_parquet(
            raw_dir, pq_dir, overwrite=cfg.overwrite, chunk_size=chunk_size
        )
        idx = hs_index_path(
            paths.meta, dataset=cfg.dataset, clCode=cfg.clCode, freqCode=cfg.freqCode
        )
        _ = update_hs_index(idx.index_file, new_codes=per_period_reporter_cmdCodes)
        if written:
            logger.info(" Parquet written: %s file(s) for %s.", len(written), period)

    return per_period_reporter_cmdCodes


@dataclass(frozen=True)
class DownloadReportperReporter:
    """
    A Dataclass for a download report per reported. Useful for manifests.
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

    # get requested months, availability df, and initialise hs_codes for metadata.
    requested_periods = build_period_list(cfg.freqCode, cfg.start, cfg.end)
    available_df = get_bulk_availability(api_key, reporterCode=reporter, cfg=cfg)
    per_reporter_cmdCodes: set[str] = set()

    # if no availability, stop.
    if available_df is None:
        logger.info(
            "No available %s data at all for reporter %s.", cfg.dataset, reporter
        )
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

    # select available periods and save missing
    available_periods = select_available_periods(requested_periods, available_df)
    available_set = set(available_periods)
    missing = [p for p in requested_periods if p not in available_set]

    # record min, max available data
    min_possible = str(available_df["period"].min()) if not available_df.empty else None
    max_possible = str(available_df["period"].max()) if not available_df.empty else None

    downloaded_or_present: list[str] = (
        []
    )  # catcher for if downloaded successfully or already there if not overwriting

    for period in available_periods:
        per_period_reporter_cmdCodes = download_one_period(
            api_key, cfg, paths, reporter=reporter, period=period
        )
        downloaded_or_present.append(period)
        per_reporter_cmdCodes.update(per_period_reporter_cmdCodes)

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
    api_key: str, cfg: ComtradeBulkConfig, paths: DataPaths
) -> MultiDownloadReport:
    """
    For a given ComtradeBulkConfig file, downloads the data in the relevant files
    with paths as basis.

    :param api_key: Comtrade API key
    :type api_key: str
    :param cfg: Configuration file for Comtrade download
    :type cfg: ComtradeBulkConfig
    :param paths: skeleton of paths ontop of which to write raw data, parquets, and metadata. Defined in src.tradeflows.core.paths
    :type paths: DataPaths
    :return: Returns a custom report containing headline information and all the individual reporter reports.
    :rtype: MultiDownloadReport
    """

    per: dict[int, DownloadReportperReporter] = (
        {}
    )  # to catch each reporter's individual report

    # loop over reporters
    for reporter in cfg.reporterCodes:
        report_per_reporter = download_bulk_range_one(
            api_key, cfg, paths, reporter=reporter
        )
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
        total_missing_periods=total_requested_periods - total_downloaded_periods,
        per_reporter=per,
    )


def build_comtrade_manifest(
    *,
    multi_report: MultiDownloadReport,
    cfg: ComtradeBulkConfig,
    paths: DataPaths,
    iso3_to_reporters: dict[str, list[int]],
    repo_root: Path,
    schema_version: str = "v 1.0.0",
) -> dict[str, Any]:
    """
    Function to build a standardised manifest for comtrade fetch.
    """
    created_utc = _utc_now_iso()

    header = {
        "schema_version": schema_version,
        "created_utc": created_utc,
        "host": {
            "user": os.getenv("USER") or os.getenv("USERNAME"),
            "machine": os.uname().nodename if hasattr(os, "uname") else None,
        },
    }

    inputs = {
        "data_source": "comtrade",
        "iso3_to_reporters": iso3_to_reporters,
        "config": _dict_ready(cfg),
    }

    # COME BACK FOR MORE


## 2. Execution code --------------------------------------


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
    """
    run_comtrade_download is the execution function per Comtrade download call.

    :param iso3_codes: List of ISO3 codes to download data for.
    :type iso3_codes: list[str]
    :param start: start date.
    :type start: str
    :param end: end date.
    :type end: str
    :param dataset: "Tariffline" or "Final" Comtrade data
    :type dataset: str
    :param freqCode: Frquency of data ("M" or "A")
    :type freqCode: str
    :param typeCode: Commodities ("C") or services ("S")
    :type typeCode: str
    :param clCode: Classification type ("HS" or other)
    :type clCode: str
    :param decompress: Whether to decompress.
    :type decompress: bool
    :param overwrite: Whether to overwrite data if it exists.
    :type overwrite: bool
    """

    # load API key
    load_dotenv()
    api_key = os.getenv("COMTRADE_API_KEY_PRIMARY")
    if not api_key:
        raise RuntimeError("Missing COMTRADE_API_KEY_PRIMARY in .env")

    # ensure dataset directories are set up
    paths = ensure_dataset_dirs("comtrade")
    _ = setup_logging(paths.logs, timestamped=True)

    mapping: dict[str, list[int]] = {}  # catcher mapping ISO3 to reporter codes
    for iso3 in iso3_codes:
        raw = comtradeapicall.convertCountryIso3ToCode(iso3)
        mapping[iso3] = as_int_list(raw)
    all_reporter_codes = sorted({code for codes in mapping.values() for code in codes})
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

    multi_report = download_bulk_range_many(api_key, cfg, paths)

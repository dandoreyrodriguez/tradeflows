from dataclasses import dataclass
import re
import pandas as pd
import yaml
from pathlib import Path

@dataclass(frozen=True)
class BulkDownloadConfig:
    """
    Configuration class.
    """
    reporter_scope: str  # "all" or "custom"
    start_period: str  # YYYYMM for monthly, YYYY for annual
    end_period: str | None   # None is latest
    typeCode: str
    clCode: str
    freqCode: str
    dataset: str
    reporterCodes: list[str] | None  # list of reporter codes if reporter_scope is "custom"

def load_config(
        config_path: str | Path,
    ) -> BulkDownloadConfig:
    """
    Load configuration from a YAML file.
    """
    with open(config_path, "r") as file:
        raw = yaml.safe_load(file)

    return BulkDownloadConfig(
        reporter_scope=raw["reporter_scope"],
        start_period=raw["start_period"],
        end_period=raw.get("end_period", None),
        typeCode=raw["typeCode"],
        clCode=raw["clCode"],
        freqCode=raw["freqCode"],
        dataset=raw["dataset"],
        reporterCodes=raw.get("reporterCodes", None),
    )

def build_period_list(
        start_period: str,
        end_period: str | None,
        freqCode: str,
    ) -> list[int]:
    """
    Build a list of periods from the start and end periods.
    """

    # monthly
    if freqCode == "M":
        # start date must be in YYYYMM format
        if not re.match(r"^\d{6}$", start_period):
            raise ValueError("start_period must be in YYYYMM format for monthly frequency.")
        # if end date is provided, it must also be in YYYYMM format
        if end_period is not None and not re.match(r"^\d{6}$", end_period):
            raise ValueError("end_period must be in YYYYMM format for monthly frequency.")
        # if end_period is None, set it to the current month
        if end_period is None:
            end_period = (pd.Timestamp.now() - pd.offsets.MonthBegin(1)).strftime("%Y%m")
        # assign start and end periods
        start = pd.Period(start_period, freq="M")
        end = pd.Period(end_period, freq="M")
        # return the list of periods
        return [int(f"{p.year}{p.month:02d}") for p in pd.period_range(start, end, freq="M")]

    # annual
    elif freqCode == "A":
        # start date must be in YYYY format
        if not re.match(r"^\d{4}$", start_period):
            raise ValueError("start_period must be in YYYY format for annual frequency.")
        # if end date is provided, it must also be in YYYY format
        if end_period is not None and not re.match(r"^\d{4}$", end_period):
            raise ValueError("end_period must be in YYYY format for annual frequency.")
        # if end_period is None, set it to the current year
        if end_period is None:
            end_period = pd.Timestamp.now().strftime("%Y")
        # assign start and end periods
        start = pd.Period(start_period, freq="Y")
        end = pd.Period(end_period, freq="Y")
        # return the list of periods
        return [int(f"{p.year}") for p in pd.period_range(start, end, freq="Y")]

    else:
        raise ValueError("Invalid freqCode. Please choose either 'M' for monthly or 'A' for annual.")


@dataclass(frozen=True)
class ComtradeJob:
    """
    Class to represent a Comtrade job.
    """
    reporterCode: str
    period: int
    typeCode: str
    clCode: str
    freqCode: str
    dataset: str
    availability_date: pd.Timestamp
    job_type: str




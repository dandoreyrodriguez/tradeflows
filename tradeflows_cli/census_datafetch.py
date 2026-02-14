######################################
# GET CENSUS HTS 10 TARIFF ESTIMATES #
######################################

from __future__ import annotations
import requests
import pandas as pd
import re
import os
from pathlib import Path
from dotenv import load_dotenv

# import own modules
from tradeflows_cli.paths import ensure_dataset_dirs, DataPaths
from tradeflows_cli.logging_setup import setup_logging, get_logger
from tradeflows_cli.hs_codes import hs_index_path, update_hs_index



logger = get_logger() # module-level logger

# more detail on codes, `CTY_CODE` can be found at "https://www.census.gov/foreign-trade/schedules/c/countrycode.html"
# extensive API documentation can be found here "https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf"

def _validate_yyyymm(date_str: str) -> None:
    """Checks date_string is 'YYYY-MM'."""
    if not re.fullmatch(r"\d{4}-\d{2}", date_str):
        raise ValueError(f"Date '{date_str}' must be 'YYYY-MM'.")


def _safe_calc_tariff(df: pd.DataFrame) -> pd.Series:
    # calc_tariff = CAL_DUT_MO / CON_VAL_MO with safe handling
    denom = pd.to_numeric(df.get("CON_VAL_MO"), errors="coerce")
    num = pd.to_numeric(df.get("CAL_DUT_MO"), errors="coerce")
    return (num / denom).where(denom > 0)


def load_handover_codes(*,dataset: str, clCode: str, freqCode: str) -> list[str]:
    paths = ensure_dataset_dirs("comtrade")
    hp = hs_index_path(paths.meta, dataset=dataset, clCode=clCode, freqCode=freqCode)
    df = pd.read_parquet(hp.index_file)
    if "cmdCode" not in df.columns:
        raise ValueError(f"Expected 'cmdCode' in {hp.index_file}. Found {list(df.columns)}.")
    return df["cmdCode"].dropna().astype(str).str.strip().tolist()

def normalise_codes(codes: list[str], *, granularity: int) -> list[str]:
    """
    Keep only codes of a given length. 
    Deal with duplicates.
    """
    if granularity not in (2,4,6,8,10):
        raise ValueError("Granularity must be one of 2,4,6,8,10.")
    
    out = sorted({c for c in codes if c.isdigit() and len(c) == granularity})
    return out

def fetch_hs_import_data(
    api_key: str,
    cmdCode: str,
    start_date: str,
    end_date: str,
    *,
    granularity: int = 10,
    fields: list[str] = [
        "CTY_NAME",
        "CON_VAL_MO",
        "CAL_DUT_MO",
        "I_COMMODITY",
        "I_COMMODITY_SDESC",
    ],
) -> pd.DataFrame:
    """
    Fetches HS US Import data from US Census API. It returns only countries, filtering out any aggregates.

    Parameters
    ----------
    api_key : str
        Census API key
    cmdCode : str
        Commodity code (must be Harmonized System code)
    start_date : str
        Start date (must be `YYYY-MM` format)
    end_date : str
        End date (must be `YYYY-MM` format)
    granularity : int, optional
        Level of granularity reported, by default 10

    Returns
    -------
    pd.DataFrame
        Raw data
    """
    # checks cmdCode has right length
    if len(cmdCode) != granularity:
        raise ValueError("Please ensure cmdCode matches granularity requested.")
    # validate cmdCode
    valid_length = {2, 4, 6, 8, 10}
    if not cmdCode.isdigit():
        raise ValueError(
            f"{cmdCode} is an invalid HS code. It must contain only digits."
        )
    if len(cmdCode) not in valid_length:
        raise ValueError(f"Cmd Codes must be a valid length (2,4,6, or 10).")

    # ensure that dates are of a `YYYY-MM` format
    _validate_yyyymm(start_date)
    _validate_yyyymm(end_date)

    # remove duplicates (if `CTY_CODE`,`I_COMMODITY`, or `COMM_LVL` are requested they
    # appear twice. Once in request and once in params)
    fields = [f for f in fields if f not in ("CTY_CODE", "I_COMMODITY", "COMM_LVL")]

    # base url
    base_url = "https://api.census.gov/data/timeseries/intltrade/imports/hs?"

    # store parameters
    params = (
        "get="
        + ",".join(fields)
        + f"&key={api_key}"
        + f"&time==from+{start_date}to+{end_date}"
        + f"&COMM_LVL=HS{str(granularity)}"
        + f"&I_COMMODITY={str(cmdCode)}"
        + "&CTY_CODE=*"
    )
    # url
    url = base_url + params
    # get request
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            break
        except Exception as e:
            logger.info(f"Attempt {attempt+1}/3 failed: {e}")

    # deal with the empty data case
    if r.status_code == 204:
        return pd.DataFrame(columns = ["CTY_CODE", "CTY_NAME","CON_VAL_MO", "CAL_DUT_MO", "I_COMMODITY","I_COMMODITY_SDESC","time"])

    # convert to data
    data = r.json()
    # to pd
    df = pd.DataFrame(data[1:], columns=data[0])

    # Check CTY_CODE has exactly 4 digits and that it begins in 1-9
    # Aggregates have other characters or begin in 0
    mask_countries = df["CTY_CODE"].astype(str).str.fullmatch(r"[1-9]\d{3}")
    df_countries = df[mask_countries].copy()

    return df_countries


def run_tariff_download(
    *,
    start_date: str,
    end_date: str,
    granularity: int = 10, 
    overwrite: bool = False,
    cmdCodes: list[str] | None = None,
    handover_dataset: str = "Tariffline",
    handover_clCode: str = "HS",
    handover_freqCode: str= "M",
) -> None:
    """
    For a list of cmdCodes and date range, produce and save parquets of tariff rates per cmdCode.
    """

    # load API key
    load_dotenv()
    api_key = os.getenv("CENSUS_API_KEY")
    if not api_key:
        raise RuntimeError("Missing CENSUS_API_KEY in .env")

    if cmdCodes and len(cmdCodes) > 0 : 
        raw_codes = cmdCodes
        source = "cli"
    else:
        raw_codes = load_handover_codes(dataset=handover_dataset, clCode=handover_clCode, freqCode=handover_freqCode)
        source = f"handover:{handover_dataset}/{handover_clCode}/{handover_freqCode}"

    cmdCodes = normalise_codes(raw_codes, granularity=granularity)
    if not cmdCodes:
        raise ValueError(f"No valid HS{granularity} codes from {source}")

    
    paths = ensure_dataset_dirs("census")
    setup_logging(paths.logs, timestamped=True)

    # loop over codes
    for cmdCode in cmdCodes:

        # create code output path
        output_file = paths.raw / f"calc_tariffs_{cmdCode}.parquet"

        if output_file.exists() and (not overwrite):
            logger.debug(f"Tariff parquet already exists for {cmdCode}. Skipping...")
            continue

        # pull data from Census api
        df = fetch_hs_import_data(api_key, cmdCode, start_date, end_date, granularity=granularity)

        if df.empty:
            logger.info("No Census data (204) for %s in %s...%s. Skipping.", cmdCode, start_date, end_date)
            continue

                # perform tariff calc
        df["CALC_TARIFF"] = _safe_calc_tariff(df)

        # select
        df_ready = df[["time", "CTY_NAME", "I_COMMODITY", "CALC_TARIFF", "CON_VAL_MO","CAL_DUT_MO"]]

        df_ready.to_parquet(output_file, index=False, compression="zstd")

if __name__ == "__main__":
    run_tariff_download(
        start_date="2025-01",
        end_date="2025-03",
        granularity=10,
        overwrite=True,
        cmdCodes=None,
        handover_dataset="Tariffline",
        handover_clCode="HS",
        handover_freqCode="M"
    )
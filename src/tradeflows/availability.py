# Script to check availability of new data using the Comtrade API
# This writes `metadataavailability_YYYYMMDD.parquet`

# load libraries
import pandas as pd
import comtradeapicall
from pathlib import Path
from tradeflows.schemas.manifest import MANIFEST_SCHEMA, KEY_COLUMNS, VALID_REPORTERS
from tradeflows.config import ComtradeJob

def availability_snapshot(
        subscription_key: str,
        period_list: list[int],
        typeCode: str,
        freqCode: str,
        clCode: str | None = None,
        reporterCode: str | None = None,
        dataset: str = "Final",
    ) -> pd.DataFrame:
    """
    Returns a snapshot of available data, filtered for list of periods.
    """

    # if Final
    if dataset == "Final":

        df = comtradeapicall.getFinalDataBulkAvailability(
            subscription_key=subscription_key,
            period=None,
            typeCode=typeCode,
            freqCode=freqCode,
            clCode=clCode,
            reporterCode=reporterCode,
        )

    # if dataset is Tariffline
    elif dataset == "Tariffline":

        df = comtradeapicall.getTarifflineDataBulkAvailability(
            subscription_key=subscription_key,
            period=None,
            typeCode=typeCode,
            freqCode=freqCode,
            clCode=clCode,
            reporterCode=reporterCode,
        )

    else:
        raise ValueError("Invalid dataset. Please choose either 'Final' or 'Tariffline'.")

    # if not pandas make pandas
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    # filter out only reporterCodes in VALID_REPORTERS
    #df = df[df["reporterCode"].isin(VALID_REPORTERS)]

    # order by reporterCode, period, typeCode, freqCode, clCode
    df = df.sort_values(by=["reporterCode", "period", "typeCode", "freqCode"])

    # filter for periods in period_list
    df = df[df["period"].isin(period_list)]

    # columns to keep
    keep_cols = ["reporterCode", "period", "typeCode", "freqCode", "publicationDate", "timestamp", "isOriginalClassification", "classificationCode"]
    add_cols = ["clCode", "dataset", "queried", "download_date", "row_count", "file_size_bytes", "status"]
    final_cols = keep_cols + add_cols
    # if df is empty, return empty df
    if df.empty:
        return pd.DataFrame(columns=final_cols)

    # add "new" columns
    df["clCode"] = clCode
    df["queried"] = pd.Timestamp.now()
    df["dataset"] = dataset
    df["download_date"] = pd.NaT
    df["row_count"] = pd.NA
    df["file_size_bytes"] = pd.NA
    df["status"] = "available"

    # return out df
    out = df[final_cols].copy().reset_index(drop=True)

    return out

def normalise_availability_snapshot(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
    """
    Normalise the availability snapshot to match the manifest schema.
    """

    # manifest schema columns
    manifest_cols = list(MANIFEST_SCHEMA.keys())

    # rename publicationDate to availability_date
    df = df.rename(columns={"publicationDate": "availability_date"})
    # convert availability_date to datetime
    df["availability_date"] = pd.to_datetime(df["availability_date"], utc = True)

    # grab only the columns in the manifest schema
    df = df[manifest_cols].copy()

    return df

def load_manifest(
        manifest_path: str | Path,
    ) -> pd.DataFrame:
    """
    Load the manifest file as a pandas DataFrame. Robust to being empty.
    """
    manifest_path = Path(manifest_path)

    if not manifest_path.exists():
        # if the manifest file does not exist, return an empty DataFrame with the correct columns
        return pd.DataFrame(columns=list(MANIFEST_SCHEMA.keys()))

    df = pd.read_parquet(manifest_path)

    missing = set(MANIFEST_SCHEMA.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Manifest file is missing columns: {missing}")

    return df


def build_job_plan_df(
        availability_df: pd.DataFrame,
        manifest_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Builds a job plan by comparing the availability snapshot with the existing manifest.
    """

    # define the key columns
    key_cols = list(KEY_COLUMNS.keys())
    manifest_cols = list(MANIFEST_SCHEMA.keys())

    # manifest if empty
    if manifest_df.empty:
        # if the manifest is empty, all availability records are new
        job_plan = availability_df.copy()
        job_plan["job_type"] = "new"
        return job_plan.reset_index(drop=True)

    # merge columns
    merge_cols = key_cols + ["availability_date"]

    merged = availability_df.merge(
        manifest_df[merge_cols],
        on=key_cols,
        how="left",
        suffixes=("_available", "_manifest"),
    )

    # new data when availability_date is not in manifest
    is_new = merged["availability_date_manifest"].isna()

    # revision data when availability_date is in manifest but is before
    # the corresponding availability_date in the available data
    is_revision = (
        merged["availability_date_manifest"].notna() &
        (merged["availability_date_available"] > merged["availability_date_manifest"])
    )

    # initialise job_type column
    merged["job_type"] = pd.NA
    # assign job types
    merged.loc[is_new, "job_type"] = "new"
    merged.loc[is_revision, "job_type"] = "revision"

    # filter for only new and revision jobs
    job_plan = merged[merged["job_type"].isin(["new", "revision"])].copy()
    # availbility_date is the new availability_date
    job_plan["availability_date"] = job_plan["availability_date_available"]
    # grab only the manifest columns and job_type
    job_plan = job_plan[manifest_cols + ["job_type"]].copy().reset_index(drop=True)

    return job_plan


def save_job_plan(
        job_plan_df: pd.DataFrame,
        job_plan_dir: str | Path,
    ) -> None:
    """
    Saves the job plan for tracking purposes. The job plan is saved as a parquet file in the specified directory.
    """
    datetime_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    job_plan_path = Path(job_plan_dir) / f"job_plan_{datetime_str}"
    job_plan_df.to_parquet(job_plan_path.with_suffix('.parquet'), index=False)
    job_plan_df.to_csv(job_plan_path.with_suffix('.csv'), index=False)  # Save as CSV for easier viewing


def build_job_plan(
        job_plan_df: pd.DataFrame,
    ) -> list[ComtradeJob]:
    """
    Converts a df of job plans into a list of ComtradeJob objects, ready for processing.
    """
    jobs = []

    for _, row in job_plan_df.iterrows():
        job = ComtradeJob(
            reporterCode=row["reporterCode"],
            period=row["period"],
            typeCode=row["typeCode"],
            clCode=row["clCode"],
            freqCode=row["freqCode"],
            dataset=row["dataset"],
            availability_date=row["availability_date"],
            job_type=row["job_type"],
        )
        jobs.append(job)
    return jobs


def update_manifest(
        manifest_df: pd.DataFrame,
        job_results_df: pd.DataFrame,
        manifest_dir: str | Path,
    ) -> None:
    """
    Updates the manifest with the results of the job processing.
    """
    # manifest cols
    manifest_cols = list(MANIFEST_SCHEMA.keys())
    # key cols
    key_cols = list(KEY_COLUMNS.keys())
    # grab manifest cols from job_results_df
    job_results_df = job_results_df[manifest_cols].copy()
    # now combine the manifest and job results
    updated_manifest_df = pd.concat([manifest_df, job_results_df], ignore_index=True)
    # make most recently data first
    updated_manifest_df = updated_manifest_df.sort_values(by=["availability_date", "download_date"], ascending=[False, False])
    # drop duplicates based on key columns, keeping the first (most recent) record
    updated_manifest_df = updated_manifest_df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    # save the updated manifest
    updated_manifest_df.to_parquet(Path(manifest_dir) / "manifest.parquet", index=False)
    updated_manifest_df.to_csv(Path(manifest_dir) / "manifest.csv", index=False)  # Save as CSV for easier viewing



# CLI
import argparse
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from tradeflows.config import load_config, build_period_list
from tradeflows.availability import availability_snapshot, normalise_availability_snapshot, load_manifest, build_job_plan_df, save_job_plan, build_job_plan, update_manifest
from tradeflows.paths import get_project_root, build_paths, ensure_paths
from tradeflows.datafetch import unit_partition, download_one_job

def parse_args():
    """
    Parse command-line arguments for updating a given configuration.
    """
    parser = argparse.ArgumentParser(description="Update configuration for bulk download.")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/monthly_2022.yaml",
        help="Path to the configuration file.",
    )

    return parser.parse_args()

def main():
    """
    Main function to execute the CLI.
    """

    # parse arguments
    args = parse_args()
    config_path = args.config

    # Load configuration
    config = load_config(config_path)
    # set the data directory
    data_dir = get_project_root() / "data"
    # make sure data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)
    # make subdirectories
    paths = build_paths(data_dir)
    ensure_paths(paths)

    # Load API key from .env
    env_path = get_project_root() / ".env"
    load_dotenv(dotenv_path=env_path)
    subscription_key = os.getenv("COMTRADE_API_KEY_PRIMARY")

    # Build list of periods
    period_list = build_period_list(
        start_period=config.start_period,
        end_period=config.end_period,
        freqCode=config.freqCode
    )

    # Parse through availability snapshot
    availability_df = availability_snapshot(
        subscription_key=subscription_key,
        period_list=period_list,
        typeCode=config.typeCode,
        freqCode=config.freqCode,
        clCode=config.clCode,
        reporterCode=None if config.reporter_scope == "all" else config.reporterCodes,
        dataset=config.dataset,
    )

    availability_df = normalise_availability_snapshot(availability_df)

    # read manifest
    manifest_df = load_manifest(paths.metadata.manifest / "manifest.parquet")

    # get jobs
    job_plan_df = build_job_plan_df(
        availability_df=availability_df,
        manifest_df=manifest_df
    )

    save_job_plan(job_plan_df, paths.metadata.jobs)

    jobs = build_job_plan(job_plan_df)

    print(f"Number of jobs to process: {len(jobs)}")

    # iterate over jobs
    results = []
    # only first job for testing
    for job in jobs[:1]:

        # ensure the raw data partition exists
        out_dir_raw = unit_partition(
            base_path=paths.raw,
            job=job
        )
        # ensure parquet data partition exists
        out_dir_parquet = unit_partition(
            base_path=paths.parquet,
            job=job
        )

        # download the data for this job
        result = download_one_job(
            subscription_key=subscription_key,
            job=job,
            out_dir_raw=out_dir_raw,
            out_dir_parquet=out_dir_parquet
        )
        results.append(result)

    # collapse results into a dataframe and save
    results_df = pd.DataFrame(results)

    # update manifest
    update_manifest(
        manifest_df=manifest_df,
        job_results_df=results_df,
        manifest_dir=paths.metadata.manifest,
    )







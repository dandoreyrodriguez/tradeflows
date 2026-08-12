# fetches the data from the Comtrade API and saves

import pandas as pd
import comtradeapicall
from contextlib import redirect_stdout
import io
from tradeflows.config import ComtradeJob
from pathlib import Path

def unit_partition(
        base_path: str | Path,
        job: ComtradeJob,
    ) -> Path:
    """
    Creates a partitioned set of directories for the raw data consistent with Hive partitioning.
    """
    p = (
        Path(base_path)
        / f"dataset={job.dataset}"
        / f"typeCode={job.typeCode}"
        / f"clCode={job.clCode}"
        / f"freqCode={job.freqCode}"
        / f"reporterCode={job.reporterCode}"
        / f"period={job.period}"
    )

    p.mkdir(parents=True, exist_ok=True)

    return p

def convert_txt_to_parquet(
        out_dir_raw: str | Path,
        out_dir_parquet: str | Path,
        sep: str = "\t",
        chunk_size: int | None = None,
    ) -> tuple[int, bool]:
    """
    Converts a group of .text files in a directory to .parquet files in another directory.
    """
    txt_files = sorted(Path(out_dir_raw).glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {out_dir_raw}")

    for txt_file in txt_files:
        processed_status = False
        row_count = 0
        try:
            # if chunk_size is None, read one .txt as a whole and convert to one .parquet
            output_stem = Path(out_dir_parquet) / "raw_comtrade"
            if chunk_size is None:
                df = pd.read_csv(txt_file, sep=sep, dtype = {"cmdCode": str})
                row_count = len(df)
                df.to_parquet(output_stem.with_suffix(".parquet"), index=False)
            else:
                partition = 0
                for chunk in pd.read_csv(txt_file, sep=sep, dtype = {"cmdCode": str}, chunksize=chunk_size):
                    row_count += len(chunk)
                    chunk.to_parquet(output_stem.with_name(f"{output_stem.stem}_part{partition}.parquet"), index=False)
                    partition += 1
            processed_status = True
        except Exception as e:
            print(f"Failed to process {txt_file}: {e}")

    if processed_status:
        print("   .txt to .parquet conversion successful!")

    return row_count, processed_status


def download_one_job(
        subscription_key: str,
        job: ComtradeJob,
        out_dir_raw: str | Path,
        out_dir_parquet: str | Path,
    ) -> None:
    """
    Downloads the data for a single job. Downloads .txt files and then converts to .parquet.
    """


    print(f"Downloading data for: reporterCode={job.reporterCode}, period={job.period}, typeCode={job.typeCode}, freqCode={job.freqCode}, clCode={job.clCode}, dataset={job.dataset}...")

    # start with .txt download
    with redirect_stdout(io.StringIO()):

        if job.dataset == "Final":
            comtradeapicall.bulkDownloadFinalFile(
                subscription_key=subscription_key,
                directory=out_dir_raw,
                period=job.period,
                typeCode=job.typeCode,
                freqCode=job.freqCode,
                clCode=job.clCode,
                reporterCode=job.reporterCode,
                decompress=True
            )
        elif job.dataset == "Tariffline":
            comtradeapicall.bulkDownloadTarifflineFile(
                subscription_key=subscription_key,
                directory=out_dir_raw,
                period=job.period,
                typeCode=job.typeCode,
                freqCode=job.freqCode,
                clCode=job.clCode,
                reporterCode=job.reporterCode,
                decompress=True
            )

    # convert the .txt files to .parquet
    row_count, processed_status = convert_txt_to_parquet(
        out_dir_raw=out_dir_raw,
        out_dir_parquet=out_dir_parquet,
        sep="\t",
        chunk_size=None
    )

    results = {
        "dataset": job.dataset,
        "reporterCode": job.reporterCode,
        "period": job.period,
        "typeCode": job.typeCode,
        "freqCode": job.freqCode,
        "clCode": job.clCode,
        "availability_date": job.availability_date,
        "download_date": pd.Timestamp.now(),
        "row_count": row_count,
        "file_size_bytes": sum(f.stat().st_size for f in Path(out_dir_parquet).glob("*.parquet")),
        "status": "success" if processed_status else "failed",
        "job_type": job.job_type,
    }
    return results

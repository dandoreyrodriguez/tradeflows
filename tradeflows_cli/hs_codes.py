#############################################
# module for read/updating/writing hs codes #
#############################################

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

@dataclass(frozen=True)
class HSCodeIndexPaths:
    index_file: Path

def hs_index_path(paths_meta: Path, *, dataset: str, clCode: str, freqCode: str) -> HSCodeIndexPaths:
    fname = f"hs_codes__datset={dataset}__cl={clCode}__freq={freqCode}.parquet"
    return HSCodeIndexPaths(index_file= paths_meta / fname)

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_hs_index(index_file) -> pd.DataFrame:

    if not index_file.exists():
        return pd.DataFrame({"cmdCode": pd.Series(dtype="string"),
                             "first_seen_utc": pd.Series(dtype="string")})
    
    df = pd.read_parquet(index_file)
    df["cmdCode"] = df["cmdCode"].astype("string")
    if "first_seen_utc" not in df.columns:
        df["first_seen_utc"] = pd.Series([None] * len(df), dtype="string")
    
    return df

    
def update_hs_index(index_file: Path, *, new_codes: set[str]) -> pd.DataFrame:

    if not new_codes:
        return load_hs_index(index_file)
    
    existing = load_hs_index(index_file)
    existing_set = set(existing["cmdCode"].dropna().astype(str))

    add = sorted({c.strip() for c in new_codes if c and c.strip()} - existing_set)
    if not add:
        return existing
    
    now = _now_utc_iso()
    add_df = pd.DataFrame({"cmdCode": pd.Series(add, dtype = "string"),
        "first_seen_utc": pd.Series([now] * len(add), dtype = "string")})
    
    out = pd.concat([existing, add_df], ignore_index=True)
    out = out.drop_duplicates(subset=["cmdCode"]).sort_values("cmdCode").reset_index(drop=True)

    tmp = index_file.with_suffix(index_file.suffix + ".tmp")
    index_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(tmp, index=False)
    tmp.replace(index_file)  # atomic on most OS/filesystems

    return out









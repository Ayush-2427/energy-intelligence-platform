#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


DEFAULT_IN = Path("data/clean/dispatch_price_parquet")
DEFAULT_OUT_FILE = Path("data/curated/dispatch_price_daily_region.parquet")


def validate_out_file(out_file: Path) -> Path:
    # If user passed a directory (or forgot .parquet), make it explicit and safe.
    # We do NOT silently write to a directory path because that caused your WinError 5 earlier.
    if out_file.exists() and out_file.is_dir():
        raise IsADirectoryError(
            f"Output path is a directory, not a file: {out_file}\n"
            f"Tip: use --out data/curated/dispatch_price_daily_region.parquet"
        )

    if out_file.suffix.lower() != ".parquet":
        raise ValueError(
            f"Output file must end with .parquet, got: {out_file}\n"
            f"Tip: use --out data/curated/dispatch_price_daily_region.parquet"
        )

    return out_file


def main(in_dir: Path, out_file: Path) -> None:
    if not in_dir.exists():
        raise FileNotFoundError(f"Parquet input not found: {in_dir}")

    out_file = validate_out_file(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_dir)

    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["rrp"] = pd.to_numeric(df["rrp"], errors="coerce")

    # Ensure required columns exist
    required = {"settlement_date", "rrp", "region_id", "date"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in parquet dataset: {sorted(missing)}")

    df = df.dropna(subset=["settlement_date", "rrp", "region_id", "date"])

    g = (
        df.groupby(["region_id", "date"], as_index=False, observed=True)
        .agg(
            intervals=("rrp", "count"),
            rrp_avg=("rrp", "mean"),
            rrp_min=("rrp", "min"),
            rrp_max=("rrp", "max"),
            rrp_std=("rrp", "std"),
        )
        .sort_values(["date", "region_id"])
        .reset_index(drop=True)
    )

    for c in ["rrp_avg", "rrp_min", "rrp_max", "rrp_std"]:
        g[c] = g[c].round(5)

    # Write atomically to avoid partial files if interrupted/locked
    tmp = out_file.with_suffix(".parquet.tmp")
    g.to_parquet(tmp, index=False)
    tmp.replace(out_file)

    print(f"Wrote daily rollups to: {out_file}")
    print(f"Rows: {len(g)} | Regions: {g['region_id'].nunique()} | Dates: {g['date'].nunique()}")
    print(g.head(10))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build daily price rollups per region from clean parquet dataset")
    ap.add_argument("--in", dest="in_dir", default=str(DEFAULT_IN), help="Input parquet dataset folder")
    ap.add_argument(
        "--out",
        dest="out_file",
        default=str(DEFAULT_OUT_FILE),
        help="Output parquet file path (must end with .parquet)",
    )
    args = ap.parse_args()
    main(Path(args.in_dir), Path(args.out_file))

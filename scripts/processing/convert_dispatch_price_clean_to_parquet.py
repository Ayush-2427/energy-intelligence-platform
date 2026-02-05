#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/clean/dispatch_price/dispatch_price_clean_batch.csv")
DEFAULT_OUTDIR = Path("data/clean/dispatch_price_parquet")


def resolve_path(p: str) -> Path:
    return Path(p)


def main(input_csv: Path, out_dir: Path) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_csv)

    # Parse settlement_date and create date column for partitioning
    df["settlement_date"] = pd.to_datetime(df["settlement_date"], format="%Y/%m/%d %H:%M:%S", errors="coerce")
    bad = df["settlement_date"].isna().sum()
    if bad:
        raise ValueError(f"Found {bad} rows with invalid settlement_date")

    df["date"] = df["settlement_date"].dt.date.astype(str)

    # Ensure numeric types
    df["rrp"] = pd.to_numeric(df["rrp"], errors="coerce")
    df["intervention"] = pd.to_numeric(df["intervention"], errors="coerce")
    df["run_no"] = pd.to_numeric(df["run_no"], errors="coerce")

    # Drop any fully broken rows
    df = df.dropna(subset=["settlement_date", "region_id", "rrp"])

    # Write partitioned parquet
    df.to_parquet(
        out_dir,
        index=False,
        partition_cols=["region_id", "date"],
    )

    print(f"Wrote parquet dataset to: {out_dir}")
    print(f"Rows: {len(df)} | Regions: {df['region_id'].nunique()} | Dates: {df['date'].nunique()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert clean dispatch price CSV into partitioned parquet dataset")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT), help="Path to clean CSV")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUTDIR), help="Output parquet dataset folder")
    args = parser.parse_args()

    main(resolve_path(args.input), resolve_path(args.out))

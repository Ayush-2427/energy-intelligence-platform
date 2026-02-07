#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import pandas as pd

from scripts.utilities.metadata_store import MetadataStore


DEFAULT_INPUT = Path("data/clean/dispatch_price/dispatch_price_clean_batch.csv")
DEFAULT_OUTDIR = Path("data/clean/dispatch_price_parquet")


def resolve_path(p: str) -> Path:
    return Path(p)


def list_partition_paths(out_dir: Path) -> list[Path]:
    """
    Pandas writes parquet partitions as nested folders like:
      out_dir/region_id=NSW1/date=2026-02-05/part-....parquet

    We treat each leaf partition folder (containing parquet files) as an artifact.
    """
    if not out_dir.exists():
        return []

    leaf_partitions: list[Path] = []
    for p in out_dir.rglob("*"):
        if not p.is_dir():
            continue

        parquet_files = list(p.glob("*.parquet"))
        if parquet_files:
            leaf_partitions.append(p)

    return sorted(set(leaf_partitions))


def main(input_csv: Path, out_dir: Path) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    out_dir.mkdir(parents=True, exist_ok=True)

    store = None
    run_id = None
    rows_written = 0

    # Metadata is optional
    try:
        if os.getenv("DATABASE_URL"):
            store = MetadataStore()
    except Exception as e:
        print(f"Metadata disabled (DB unavailable): {e}")
        store = None

    # Start run tracking
    if store:
        try:
            run_id = store.start_run(
                command="parquet",
                raw_dir=str(input_csv.parent),
                max_files=None,
                cleanup=False,
            )
        except Exception as e:
            print(f"Metadata disabled (failed to start run): {e}")
            store = None
            run_id = None

    try:
        df = pd.read_csv(input_csv)

        df["settlement_date"] = pd.to_datetime(
            df["settlement_date"],
            format="%Y/%m/%d %H:%M:%S",
            errors="coerce",
        )
        bad = df["settlement_date"].isna().sum()
        if bad:
            raise ValueError(f"Found {bad} rows with invalid settlement_date")

        df["date"] = df["settlement_date"].dt.date.astype(str)

        df["rrp"] = pd.to_numeric(df["rrp"], errors="coerce")
        df["intervention"] = pd.to_numeric(df["intervention"], errors="coerce")
        df["run_no"] = pd.to_numeric(df["run_no"], errors="coerce")

        df = df.dropna(subset=["settlement_date", "region_id", "rrp"])

        df.to_parquet(
            out_dir,
            index=False,
            partition_cols=["region_id", "date"],
        )

        rows_written = len(df)

        print(f"Wrote parquet dataset to: {out_dir}")
        print(f"Rows: {len(df)} | Regions: {df['region_id'].nunique()} | Dates: {df['date'].nunique()}")

        if store and run_id:
            try:
                input_artifact_id = store.register_artifact(run_id, "clean_csv", input_csv)

                partitions = list_partition_paths(out_dir)
                for part_dir in partitions:
                    part_artifact_id = store.register_artifact(run_id, "parquet_partition", part_dir)
                    store.add_lineage(run_id, "artifact", input_artifact_id, part_artifact_id)

            except Exception as e:
                print(f"Metadata warn: failed to record parquet artifacts/lineage: {e}")

        if store and run_id:
            store.finish_run(run_id, status="success", rows_appended=rows_written, error_message=None)

    except Exception as e:
        if store and run_id:
            try:
                store.finish_run(run_id, status="failed", rows_appended=rows_written, error_message=str(e))
            except Exception:
                pass
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert clean dispatch price CSV into partitioned parquet dataset")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT), help="Path to clean CSV")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUTDIR), help="Output parquet dataset folder")
    args = parser.parse_args()

    main(resolve_path(args.input), resolve_path(args.out))

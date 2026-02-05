import argparse
import pandas as pd
from pathlib import Path


DEFAULT_CLEAN_CSV = Path("data/clean/dispatch_price/dispatch_price_clean_batch.csv")
DEFAULT_CURATED_BASE_DIR = Path("data/curated/dispatch_price")

DEDUP_KEYS = ["settlement_date", "region_id", "run_no"]


def main(clean_csv: Path, curated_dir: Path, partition: bool) -> None:
    if not clean_csv.exists():
        raise FileNotFoundError(f"Clean batch file not found: {clean_csv}")

    curated_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(clean_csv)
    if df.empty:
        raise ValueError("Clean batch CSV is empty. Aborting.")

    df["settlement_date"] = pd.to_datetime(df["settlement_date"])

    original_count = len(df)

    df = (
        df.sort_values(by=DEDUP_KEYS)
        .drop_duplicates(subset=DEDUP_KEYS, keep="last")
    )

    deduped_count = len(df)

    df = df.sort_values(by=["settlement_date", "region_id"], ascending=[True, True])

    if not partition:
        out_file = curated_dir / "dispatch_price_batch.parquet"
        df.to_parquet(out_file, engine="pyarrow", index=False)
        print(f"Wrote curated parquet file to {out_file}")
        print(f"Input rows: {original_count}")
        print(f"Rows after deduplication: {deduped_count}")
        return

    df["settlement_day"] = df["settlement_date"].dt.date

    for day, day_df in df.groupby("settlement_day"):
        partition_dir = curated_dir / f"settlement_date={day}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "part-000.parquet"

        day_df = day_df.drop(columns=["settlement_day"])
        day_df.to_parquet(output_file, engine="pyarrow", index=False)

        print(f"Wrote {len(day_df)} rows to {output_file}")

    print(f"Input rows: {original_count}")
    print(f"Rows after deduplication: {deduped_count}")
    print("Partitioned Parquet write complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert clean batch dispatch CSV to parquet")
    parser.add_argument("--input", type=str, default=str(DEFAULT_CLEAN_CSV), help="Path to clean batch CSV")
    parser.add_argument("--out", type=str, default=str(DEFAULT_CURATED_BASE_DIR), help="Curated output directory")
    parser.add_argument("--partition", action="store_true", help="Write hive-style partitioned parquet")

    args = parser.parse_args()
    main(clean_csv=Path(args.input), curated_dir=Path(args.out), partition=args.partition)

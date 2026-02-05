import argparse
import csv
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Optional, Set, Tuple


DEFAULT_RAW_DIR = Path("data/raw/dispatch_inbox")
OUTPUT_DIR = Path("data/clean/dispatch_price")
OUTPUT_FILE = OUTPUT_DIR / "dispatch_price_clean_batch.csv"


def resolve_raw_dir(cli_raw_dir: str | None) -> Path:
    if cli_raw_dir:
        return Path(cli_raw_dir)
    env_raw = os.getenv("RAW_DIR")
    if env_raw:
        return Path(env_raw)
    return DEFAULT_RAW_DIR


def normalize_header(h: str) -> str:
    return h.strip().strip('"').upper().replace(" ", "").replace("_", "")


def pick(mapping: Dict[str, int], *candidates: str) -> Optional[int]:
    for c in candidates:
        key = normalize_header(c)
        if key in mapping:
            return mapping[key]
    return None


def key_tuple(row: dict) -> Tuple[str, str, str, str]:
    return (
        str(row["settlement_date"]),
        str(row["region_id"]),
        str(row["run_no"]),
        str(row["intervention"]),
    )


def load_existing_keys(path: Path) -> Set[Tuple[str, str, str, str]]:
    if not path.exists():
        return set()

    keys: Set[Tuple[str, str, str, str]] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r or "settlement_date" not in r:
                continue
            keys.add(
                (
                    r.get("settlement_date", ""),
                    r.get("region_id", ""),
                    r.get("run_no", ""),
                    r.get("intervention", ""),
                )
            )
    return keys


def process_csv_with_schema(csv_path: Path) -> list[dict]:
    rows_out: list[dict] = []
    schemas: Dict[str, Dict[str, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue

            row0 = row[0].lstrip("\ufeff").strip()
            if len(row) < 2:
                continue

            rec = row[1].strip()

            if row0.upper() == "I":
                mapping: Dict[str, int] = {}
                for idx, col in enumerate(row):
                    key = normalize_header(col)
                    if key:
                        mapping[key] = idx
                schemas[rec] = mapping
                continue

            if row0.upper() != "D":
                continue

            mapping = schemas.get(rec)
            if not mapping:
                continue

            idx_region = pick(mapping, "REGIONID", "REGION_ID", "REGION")
            idx_rrp = pick(mapping, "RRP", "REGIONPRICE", "PRICE")
            idx_settle = pick(mapping, "SETTLEMENTDATE", "SETTLEMENT_DATE", "DATETIME", "INTERVAL_DATETIME")
            idx_runno = pick(mapping, "RUNNO", "RUN_NO")
            idx_intervention = pick(mapping, "INTERVENTION")

            if idx_region is None or idx_rrp is None or idx_settle is None:
                continue

            try:
                settlement_date = row[idx_settle].strip().strip('"')
                region_id = row[idx_region].strip().strip('"')
                rrp = float(row[idx_rrp])

                run_no = int(row[idx_runno]) if idx_runno is not None and row[idx_runno] else 0
                intervention = int(row[idx_intervention]) if idx_intervention is not None and row[idx_intervention] else 0

                rows_out.append(
                    {
                        "settlement_date": settlement_date,
                        "region_id": region_id,
                        "rrp": rrp,
                        "intervention": intervention,
                        "run_no": run_no,
                    }
                )
            except Exception:
                continue

    return rows_out


def list_inbox_files(raw_dir: Path) -> tuple[list[Path], list[Path]]:
    csv_candidates = list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.CSV"))
    zip_candidates = list(raw_dir.glob("*.zip")) + list(raw_dir.glob("*.ZIP"))

    csv_files = sorted({p.resolve() for p in csv_candidates})
    zip_files = sorted({p.resolve() for p in zip_candidates})

    return [Path(p) for p in csv_files], [Path(p) for p in zip_files]


def extract_csvs_from_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            base = Path(info.filename).name
            if not base:
                continue
            if base.lower().endswith(".csv"):
                target_path = extract_dir / base
                with zf.open(info, "r") as src, target_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                extracted.append(target_path)
    return sorted(extracted)


def append_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["settlement_date", "region_id", "rrp", "intervention", "run_no"],
        )
        if not file_exists:
            writer.writeheader()

        for r in rows:
            writer.writerow(
                {
                    "settlement_date": r["settlement_date"],
                    "region_id": r["region_id"],
                    "rrp": r["rrp"],
                    "intervention": r["intervention"],
                    "run_no": r["run_no"],
                }
            )


def main(raw_dir: Path, cleanup: bool, max_files: int | None) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    existing_keys = load_existing_keys(OUTPUT_FILE)
    if existing_keys:
        print(f"Existing output rows: {len(existing_keys)} (used for dedupe)")

    csv_files, zip_files = list_inbox_files(raw_dir)

    print(f"Raw dir: {raw_dir}")
    print(f"Found {len(csv_files)} loose CSV files")
    print(f"Found {len(zip_files)} ZIP files")
    if max_files is not None:
        print(f"Max inbox files to process this run: {max_files}")

    if not csv_files and not zip_files:
        print(f"No dispatch CSV or ZIP files found in: {raw_dir}")
        return

    new_rows: list[dict] = []
    processed_csv_files: list[Path] = []
    processed_zip_files: list[Path] = []

    processed_count = 0

    with tempfile.TemporaryDirectory(prefix="aemo_dispatch_extract_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)

        try:
            for i, csv_file in enumerate(csv_files, start=1):
                if max_files is not None and processed_count >= max_files:
                    print("Reached max files cap. Stopping early.")
                    break

                print(f"[CSV {i}/{len(csv_files)}] Processing: {csv_file.name}")
                rows = process_csv_with_schema(csv_file)

                kept = 0
                for r in rows:
                    kt = key_tuple(r)
                    if kt not in existing_keys:
                        existing_keys.add(kt)
                        new_rows.append(r)
                        kept += 1

                # Always mark as processed if we successfully read it
                processed_csv_files.append(csv_file)

                processed_count += 1

            for j, zip_file in enumerate(zip_files, start=1):
                if max_files is not None and processed_count >= max_files:
                    print("Reached max files cap. Stopping early.")
                    break

                print(f"[ZIP {j}/{len(zip_files)}] Extracting: {zip_file.name}")

                extracted_dir = tmp_dir / zip_file.stem
                extracted_dir.mkdir(parents=True, exist_ok=True)

                extracted_csvs = extract_csvs_from_zip(zip_file, extracted_dir)
                if not extracted_csvs:
                    print("  No CSVs in this zip. Skipping.")
                    # Still mark the zip as processed so cleanup can delete it if requested
                    processed_zip_files.append(zip_file)
                    processed_count += 1
                    continue

                zip_kept = 0
                for k, extracted_csv in enumerate(extracted_csvs, start=1):
                    print(f"  [ZIP CSV {k}/{len(extracted_csvs)}] Reading: {extracted_csv.name}")
                    rows = process_csv_with_schema(extracted_csv)
                    for r in rows:
                        kt = key_tuple(r)
                        if kt not in existing_keys:
                            existing_keys.add(kt)
                            new_rows.append(r)
                            zip_kept += 1

                # Always mark the zip as processed if we could open it
                processed_zip_files.append(zip_file)

                if zip_kept > 0:
                    print(f"  Added {zip_kept} new rows from this zip")
                else:
                    print("  No new rows from this zip (all duplicates or no region price rows)")

                processed_count += 1

            if new_rows:
                append_rows(OUTPUT_FILE, new_rows)
                print(f"Appended new rows: {len(new_rows)}")
                print(f"Output file: {OUTPUT_FILE}")
            else:
                print("No new rows to append. Output unchanged.")

            # Cleanup must run even if there were 0 new rows
            if cleanup:
                for file in processed_csv_files:
                    file.unlink(missing_ok=True)
                for file in processed_zip_files:
                    file.unlink(missing_ok=True)
                print("Cleanup complete.")
            else:
                print("Raw files kept (no cleanup).")

        except KeyboardInterrupt:
            print("Interrupted by user. Raw files were NOT deleted.")
            raise
        except Exception:
            print("ERROR: Batch processing failed.")
            print("Raw files were NOT deleted.")
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch process AEMO dispatch price CSV and ZIP files")
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=None,
        help='Raw inbox folder (default: env RAW_DIR or "data/raw/dispatch_inbox")',
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete raw CSV and ZIP files after processing",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=50,
        help="Process at most N inbox files this run (default 50)",
    )

    args = parser.parse_args()
    raw_dir = resolve_raw_dir(args.raw_dir)
    main(raw_dir=raw_dir, cleanup=args.cleanup, max_files=args.max_files)

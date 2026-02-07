from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

from scripts.utilities.dispatch_price_repo import upsert_dispatch_prices
from scripts.utilities.metadata_store import MetadataStore
from scripts.utilities.watermark_store import get_watermark, set_watermark


DEFAULT_RAW_DIR = Path("data/raw/dispatch_inbox")

WATERMARK_KEY = "dispatch_price_zip_ts"
ZIP_TS_RE = re.compile(r"PUBLIC_DISPATCHIS_(\d{12})_")


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


def parse_zip_ts(zip_filename: str) -> Optional[str]:
    """
    Extract YYYYMMDDHHMM from filename. Returns string like '202602052110' or None.
    """
    m = ZIP_TS_RE.search(zip_filename)
    if not m:
        return None
    return m.group(1)


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


def try_start_metadata_run(
    store: Optional[MetadataStore],
    raw_dir: Path,
    max_files: Optional[int],
    cleanup: bool,
) -> tuple[Optional[MetadataStore], Optional[str]]:
    if not store:
        return None, None

    try:
        run_id = store.start_run(command="batch_db", raw_dir=str(raw_dir), max_files=max_files, cleanup=cleanup)
        return store, run_id
    except Exception as e:
        print(f"Metadata disabled (failed to start run): {e}")
        return None, None


def main(raw_dir: Path, cleanup: bool, max_files: int | None) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Read watermark (YYYYMMDDHHMM string)
    watermark = get_watermark(WATERMARK_KEY)
    if watermark:
        print(f"Watermark: {WATERMARK_KEY}={watermark}")
    else:
        print(f"Watermark: {WATERMARK_KEY} is not set (first run behavior)")

    # Optional metadata tracking
    store: Optional[MetadataStore] = None
    run_id: Optional[str] = None

    try:
        if os.getenv("DATABASE_URL"):
            store = MetadataStore()
    except Exception as e:
        print(f"Metadata disabled (DB unavailable): {e}")
        store = None

    store, run_id = try_start_metadata_run(store, raw_dir, max_files, cleanup)

    csv_files, zip_files = list_inbox_files(raw_dir)

    print(f"Raw dir: {raw_dir}")
    print(f"Found {len(csv_files)} loose CSV files")
    print(f"Found {len(zip_files)} ZIP files")
    if max_files is not None:
        print(f"Max inbox files to process this run: {max_files}")

    if not csv_files and not zip_files:
        print(f"No dispatch CSV or ZIP files found in: {raw_dir}")
        if store and run_id:
            store.finish_run(run_id, status="success", rows_appended=0, error_message=None)
        return

    processed_csv_files: list[Path] = []
    processed_zip_files: list[Path] = []
    processed_count = 0

    total_rows_parsed = 0
    total_inserted = 0
    total_updated = 0

    # Track max ZIP ts processed this run, to advance watermark at end
    max_zip_ts_processed: Optional[str] = None

    try:
        with tempfile.TemporaryDirectory(prefix="aemo_dispatch_extract_") as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)

            # 1) Loose CSVs (no watermark logic, because watermark is for ZIP filenames)
            for i, csv_file in enumerate(csv_files, start=1):
                if max_files is not None and processed_count >= max_files:
                    print("Reached max files cap. Stopping early.")
                    break

                print(f"[CSV {i}/{len(csv_files)}] Processing: {csv_file.name}")

                if store and run_id:
                    try:
                        store.register_raw_object(run_id, "csv", csv_file)
                    except Exception as e:
                        print(f"Metadata warn: could not register raw csv: {e}")

                rows = process_csv_with_schema(csv_file)
                total_rows_parsed += len(rows)

                inserted, updated = upsert_dispatch_prices(rows)
                total_inserted += inserted
                total_updated += updated
                print(f"  Parsed: {len(rows)} | Inserted: {inserted} | Updated: {updated}")

                processed_csv_files.append(csv_file)
                processed_count += 1

            # 2) ZIPs with watermark
            for j, zip_file in enumerate(zip_files, start=1):
                if max_files is not None and processed_count >= max_files:
                    print("Reached max files cap. Stopping early.")
                    break

                zip_ts = parse_zip_ts(zip_file.name)
                print(f"[ZIP {j}/{len(zip_files)}] Extracting: {zip_file.name}")

                # Fast skip by watermark if we can parse timestamp
                if watermark and zip_ts and zip_ts <= watermark:
                    print("  Skipping (older than or equal to watermark)")
                    continue

                # Register raw zip (this also gives you sha dedupe behavior, if you implemented it there)
                already_ingested = False
                if store and run_id:
                    try:
                        # If your MetadataStore.register_raw_object has sha unique constraint handling,
                        # it should raise or return a signal. If not, we still proceed and sha dedupe
                        # may happen elsewhere.
                        store.register_raw_object(run_id, "zip", zip_file)
                    except Exception as e:
                        # If your MetadataStore raises something like "already exists", treat as skip
                        msg = str(e).lower()
                        if "already ingested" in msg or "duplicate" in msg or "unique" in msg:
                            already_ingested = True
                        else:
                            print(f"Metadata warn: could not register raw zip: {e}")

                if already_ingested:
                    print("  Skipping (already ingested by sha256)")
                    continue

                extracted_dir = tmp_dir / zip_file.stem
                extracted_dir.mkdir(parents=True, exist_ok=True)

                extracted_csvs = extract_csvs_from_zip(zip_file, extracted_dir)
                if not extracted_csvs:
                    print("  No CSVs in this zip. Skipping.")
                    processed_zip_files.append(zip_file)
                    processed_count += 1
                    continue

                zip_parsed = 0
                zip_inserted = 0
                zip_updated = 0

                for k, extracted_csv in enumerate(extracted_csvs, start=1):
                    print(f"  [ZIP CSV {k}/{len(extracted_csvs)}] Reading: {extracted_csv.name}")
                    rows = process_csv_with_schema(extracted_csv)
                    zip_parsed += len(rows)

                    inserted, updated = upsert_dispatch_prices(rows)
                    zip_inserted += inserted
                    zip_updated += updated

                total_rows_parsed += zip_parsed
                total_inserted += zip_inserted
                total_updated += zip_updated

                print(f"  Parsed from zip: {zip_parsed} | Inserted: {zip_inserted} | Updated: {zip_updated}")

                # Advance max zip ts processed if we ingested anything or even just attempted processing
                if zip_ts:
                    if (max_zip_ts_processed is None) or (zip_ts > max_zip_ts_processed):
                        max_zip_ts_processed = zip_ts

                processed_zip_files.append(zip_file)
                processed_count += 1

        if cleanup:
            for file in processed_csv_files:
                file.unlink(missing_ok=True)
            for file in processed_zip_files:
                file.unlink(missing_ok=True)
            print("Cleanup complete.")
        else:
            print("Raw files kept (no cleanup).")

        # Advance watermark only if we processed newer zip timestamps
        if max_zip_ts_processed:
            set_watermark(WATERMARK_KEY, max_zip_ts_processed)
            print(f"Watermark advanced: {WATERMARK_KEY}={max_zip_ts_processed}")

        # Record run result
        rows_appended = total_inserted + total_updated
        if store and run_id:
            store.finish_run(run_id, status="success", rows_appended=rows_appended, error_message=None)

        print(f"Done. Parsed: {total_rows_parsed} | Inserted: {total_inserted} | Updated: {total_updated}")

    except KeyboardInterrupt:
        print("Interrupted by user. Raw files were NOT deleted.")
        if store and run_id:
            store.finish_run(run_id, status="failed", rows_appended=(total_inserted + total_updated), error_message="KeyboardInterrupt")
        raise
    except Exception as e:
        print("ERROR: Batch processing failed.")
        print("Raw files were NOT deleted.")
        if store and run_id:
            store.finish_run(run_id, status="failed", rows_appended=(total_inserted + total_updated), error_message=str(e))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch process AEMO dispatch price CSV and ZIP files (DB-first + watermark)")
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

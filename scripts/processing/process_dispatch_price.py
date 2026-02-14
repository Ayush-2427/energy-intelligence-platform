# scripts/processing/process_dispatch_price.py
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

from scripts.utilities.db import get_conn
from scripts.utilities.dispatch_price_repo import upsert_dispatch_prices


DEFAULT_RAW_DIR = Path("data/raw/dispatch_inbox")
PROCESSED_WATERMARK_NAME = "dispatch_price_processed_ts"

ZIP_TS_RE = re.compile(r"PUBLIC_DISPATCHIS_(\d{12})_")


def resolve_raw_dir(cli_raw_dir: Optional[str]) -> Path:
    if cli_raw_dir:
        return Path(cli_raw_dir)
    env_raw = os.getenv("RAW_DIR")
    if env_raw:
        return Path(env_raw)
    return DEFAULT_RAW_DIR


def parse_zip_ts(zip_filename: str) -> Optional[str]:
    m = ZIP_TS_RE.search(zip_filename)
    return m.group(1) if m else None


def normalize_header(h: str) -> str:
    return h.strip().strip('"').upper().replace(" ", "").replace("_", "")


def pick(mapping: Dict[str, int], *candidates: str) -> Optional[int]:
    for c in candidates:
        k = normalize_header(c)
        if k in mapping:
            return mapping[k]
    return None


def process_csv_with_schema(csv_path: Path) -> Tuple[list[dict], int]:
    """
    Returns (rows_out, invalid_count)

    rows_out dict schema:
      settlement_date (string as in file), region_id, rrp(float), intervention(int), run_no(int)
    """
    rows_out: list[dict] = []
    invalid = 0
    schemas: Dict[str, Dict[str, int]] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 2:
                continue

            row0 = row[0].lstrip("\ufeff").strip()
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
                invalid += 1

    return rows_out, invalid


def list_inbox_files(raw_dir: Path) -> Tuple[list[Path], list[Path]]:
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


def create_pipeline_run_metrics(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into pipeline_run_metrics (run_type)
            values ('dispatch_price_processing')
            returning id;
            """
        )
        run_id = int(cur.fetchone()[0])
    conn.commit()
    return run_id


def finalize_pipeline_run_metrics(conn, run_id: int, status: str, metrics: dict, error_message: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update pipeline_run_metrics
            set
                finished_at = now(),
                status = %s,
                total_files = %s,
                total_rows = %s,
                total_inserted = %s,
                total_updated = %s,
                total_invalid = %s,
                error_message = %s
            where id = %s;
            """,
            (
                status,
                metrics["files"],
                metrics["rows"],
                metrics["inserted"],
                metrics["updated"],
                metrics["invalid"],
                error_message,
                run_id,
            ),
        )
    conn.commit()


def get_processed_watermark(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select value
            from watermark_state
            where name = %s
            limit 1;
            """,
            (PROCESSED_WATERMARK_NAME,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def set_processed_watermark(conn, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into watermark_state (name, value, updated_at)
            values (%s, %s, now())
            on conflict (name)
            do update set value = excluded.value, updated_at = now();
            """,
            (PROCESSED_WATERMARK_NAME, value),
        )
    conn.commit()


def main(raw_dir: Path, cleanup: bool, max_files: Optional[int]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)

    metrics = {"files": 0, "rows": 0, "inserted": 0, "updated": 0, "invalid": 0}

    with get_conn() as conn:
        run_id = create_pipeline_run_metrics(conn)
        error_message: Optional[str] = None

        try:
            processed_gate = get_processed_watermark(conn)
            if processed_gate:
                print(f"Watermark: {PROCESSED_WATERMARK_NAME}={processed_gate}")
            else:
                print(f"Watermark: {PROCESSED_WATERMARK_NAME} is not set (first run behavior)")

            csv_files, zip_files = list_inbox_files(raw_dir)

            print(f"Raw dir: {raw_dir}")
            print(f"Found {len(csv_files)} loose CSV files")
            print(f"Found {len(zip_files)} ZIP files")
            if max_files is not None:
                print(f"Max inbox files to process this run: {max_files}")

            processed_csv_files: list[Path] = []
            processed_zip_files: list[Path] = []
            processed_count = 0
            max_zip_ts_processed: Optional[str] = None

            # Loose CSVs (optional). We do not advance watermark based on loose CSVs.
            for i, csv_file in enumerate(csv_files, start=1):
                if max_files is not None and processed_count >= max_files:
                    print("Reached max files cap. Stopping early.")
                    break

                print(f"[CSV {i}/{len(csv_files)}] Processing: {csv_file.name}")
                rows, invalid = process_csv_with_schema(csv_file)

                metrics["invalid"] += invalid
                metrics["rows"] += len(rows)

                inserted, updated = upsert_dispatch_prices(rows)
                metrics["inserted"] += inserted
                metrics["updated"] += updated

                print(f"  Parsed: {len(rows)} | Inserted: {inserted} | Updated: {updated}")

                processed_csv_files.append(csv_file)
                metrics["files"] += 1
                processed_count += 1

            # ZIPs (watermark gated)
            with tempfile.TemporaryDirectory(prefix="aemo_dispatch_extract_") as tmp_dir_str:
                tmp_dir = Path(tmp_dir_str)

                for j, zip_file in enumerate(zip_files, start=1):
                    if max_files is not None and processed_count >= max_files:
                        print("Reached max files cap. Stopping early.")
                        break

                    zip_ts = parse_zip_ts(zip_file.name)
                    print(f"[ZIP {j}/{len(zip_files)}] Extracting: {zip_file.name}")

                    if processed_gate and zip_ts and zip_ts <= processed_gate:
                        print("  Skipping (older than or equal to processed watermark)")
                        continue

                    extracted_dir = tmp_dir / zip_file.stem
                    extracted_dir.mkdir(parents=True, exist_ok=True)

                    extracted_csvs = extract_csvs_from_zip(zip_file, extracted_dir)
                    if not extracted_csvs:
                        print("  No CSVs in this zip. Skipping.")
                        processed_zip_files.append(zip_file)
                        metrics["files"] += 1
                        processed_count += 1
                        continue

                    zip_parsed = 0
                    zip_invalid = 0
                    zip_inserted = 0
                    zip_updated = 0

                    for k, extracted_csv in enumerate(extracted_csvs, start=1):
                        print(f"  [ZIP CSV {k}/{len(extracted_csvs)}] Reading: {extracted_csv.name}")
                        rows, invalid = process_csv_with_schema(extracted_csv)

                        zip_parsed += len(rows)
                        zip_invalid += invalid

                        inserted, updated = upsert_dispatch_prices(rows)
                        zip_inserted += inserted
                        zip_updated += updated

                    metrics["rows"] += zip_parsed
                    metrics["invalid"] += zip_invalid
                    metrics["inserted"] += zip_inserted
                    metrics["updated"] += zip_updated

                    print(f"  Parsed from zip: {zip_parsed} | Inserted: {zip_inserted} | Updated: {zip_updated}")

                    if zip_ts and (max_zip_ts_processed is None or zip_ts > max_zip_ts_processed):
                        max_zip_ts_processed = zip_ts

                    processed_zip_files.append(zip_file)
                    metrics["files"] += 1
                    processed_count += 1

            if cleanup:
                for f in processed_csv_files:
                    f.unlink(missing_ok=True)
                for f in processed_zip_files:
                    f.unlink(missing_ok=True)
                print("Cleanup complete.")
            else:
                print("Raw files kept (no cleanup).")

            if max_zip_ts_processed:
                set_processed_watermark(conn, max_zip_ts_processed)
                print(f"Watermark advanced: {PROCESSED_WATERMARK_NAME}={max_zip_ts_processed}")

            finalize_pipeline_run_metrics(conn, run_id, "success", metrics, None)

            print(f"Done. Parsed: {metrics['rows']} | Inserted: {metrics['inserted']} | Updated: {metrics['updated']}")

        except Exception as e:
            error_message = str(e)
            finalize_pipeline_run_metrics(conn, run_id, "failed", metrics, error_message)
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process AEMO dispatch price CSV and ZIP files into DB (watermark + metrics)")
    parser.add_argument("--raw-dir", type=str, default=None, help='Raw inbox folder (default: env RAW_DIR or "data/raw/dispatch_inbox")')
    parser.add_argument("--cleanup", action="store_true", help="Delete processed raw CSV and ZIP files after processing")
    parser.add_argument("--max-files", type=int, default=50, help="Process at most N inbox files this run (default 50)")
    args = parser.parse_args()

    main(raw_dir=resolve_raw_dir(args.raw_dir), cleanup=args.cleanup, max_files=args.max_files)

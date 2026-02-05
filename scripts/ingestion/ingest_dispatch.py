#!/usr/bin/env python3
import argparse
import hashlib
import json
import logging
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import requests

DEFAULT_TIMEOUT = (10, 120)
DEFAULT_REPORT = "DispatchIS_Reports"
BASE_NEMWEB_CURRENT = "https://www.nemweb.com.au/REPORTS/CURRENT/"

REPORT_PATTERNS = {
    "DispatchIS_Reports": re.compile(r"^PUBLIC_DISPATCHIS_(\d{12})(?:_\d{16})?\.zip$", re.IGNORECASE),
    "Dispatch_SCADA": re.compile(r"^PUBLIC_DISPATCHSCADA_(\d{12})(?:_\d{16})?\.zip$", re.IGNORECASE),
}


@dataclass(frozen=True)
class Paths:
    project_root: Path
    inbox_dir: Path
    state_dir: Path
    watermark_file: Path


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        scripts_ok = (p / "scripts").is_dir()
        markers_ok = (p / "docker").is_dir() or (p / "requirements.txt").exists() or (p / "metadata").is_dir()
        if scripts_ok and markers_ok:
            return p
    if Path("/app").exists():
        return Path("/app")
    return start.parents[2]


def resolve_paths() -> Paths:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)

    raw_dir_env = os.environ.get("RAW_DIR")
    if raw_dir_env:
        raw_dir_path = Path(raw_dir_env)
        inbox_dir = raw_dir_path if raw_dir_path.is_absolute() else (project_root / raw_dir_path)
    else:
        inbox_dir = project_root / "data" / "raw" / "dispatch_inbox"

    state_dir = project_root / "data" / "raw" / "_ingestion_state"
    watermark_file = state_dir / "watermark.json"
    return Paths(project_root, inbox_dir, state_dir, watermark_file)


def ensure_dirs(paths: Paths) -> None:
    paths.inbox_dir.mkdir(parents=True, exist_ok=True)
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    if not paths.watermark_file.exists():
        paths.watermark_file.write_text(
            json.dumps({"last_ingested_yyyymmddhhmm": None}, indent=2),
            encoding="utf-8",
        )


def get_report_name(cli_report: Optional[str]) -> str:
    report = (cli_report or os.environ.get("AEMO_REPORT") or DEFAULT_REPORT).strip()
    if report not in REPORT_PATTERNS:
        valid = ", ".join(sorted(REPORT_PATTERNS.keys()))
        raise ValueError(f"Unsupported report '{report}'. Valid: {valid}")
    return report


def report_url(report: str) -> str:
    return f"{BASE_NEMWEB_CURRENT}{report}/"


def fetch_directory_listing(session: requests.Session, url: str) -> str:
    logging.info(f"Fetching listing: {url}")
    r = session.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.text


def extract_zip_links(html: str) -> List[str]:
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    zips: List[str] = []
    for h in hrefs:
        name = h.strip().split("/")[-1]
        if name.lower().endswith(".zip"):
            zips.append(name)
    return sorted(set(zips))


def parse_ts_from_name(name: str, report: str) -> Optional[datetime]:
    m = REPORT_PATTERNS[report].match(name)
    if not m:
        return None
    ts = m.group(1)
    try:
        return datetime.strptime(ts, "%Y%m%d%H%M")
    except ValueError:
        return None


def load_watermark(path: Path) -> Optional[datetime]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        v = obj.get("last_ingested_yyyymmddhhmm")
        if not v:
            return None
        return datetime.strptime(v, "%Y%m%d%H%M")
    except Exception:
        return None


def save_watermark(path: Path, dt: datetime) -> None:
    payload = {"last_ingested_yyyymmddhhmm": dt.strftime("%Y%m%d%H%M")}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def validate_zip(path: Path) -> Tuple[bool, str]:
    if not path.exists():
        return False, "file_missing"
    if path.stat().st_size <= 0:
        return False, "empty_file"
    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                return False, f"bad_member:{bad}"
    except zipfile.BadZipFile:
        return False, "bad_zip"
    except Exception as e:
        return False, f"zip_error:{type(e).__name__}"
    return True, "ok"


def download_zip(
    session: requests.Session,
    base_url: str,
    filename: str,
    inbox_dir: Path,
    dry_run: bool,
    retries: int = 3,
    backoff_sec: float = 1.5,
) -> Path:
    url = base_url + filename
    final_path = inbox_dir / filename
    tmp_path = inbox_dir / (filename + ".tmp")

    if final_path.exists():
        logging.info(f"Already exists on disk, skipping: {filename}")
        return final_path

    if dry_run:
        logging.info(f"[DRY RUN] Would download: {url} -> {final_path}")
        return final_path

    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Downloading ({attempt}/{retries}): {filename}")
            with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as r:
                r.raise_for_status()
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            ok, reason = validate_zip(tmp_path)
            if not ok:
                logging.error(f"Validation failed for {filename}: {reason}. Deleting temp file.")
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"invalid_zip:{reason}")

            tmp_path.replace(final_path)
            digest = sha256_file(final_path)
            logging.info(f"Saved: {final_path} | sha256={digest}")
            return final_path

        except Exception as e:
            logging.warning(f"Download attempt failed for {filename}: {e}")
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            if attempt < retries:
                sleep_for = backoff_sec * attempt
                logging.info(f"Retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)
            else:
                raise

    return final_path


def effective_gate(
    watermark: Optional[datetime],
    since_arg: Optional[str],
    backfill_days: Optional[int],
) -> Optional[datetime]:
    if since_arg:
        return datetime.strptime(since_arg, "%Y%m%d%H%M")
    if backfill_days is not None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return now - timedelta(days=backfill_days)
    return watermark


def pick_candidates(
    all_files: List[str],
    report: str,
    gate_dt: Optional[datetime],
    limit: int,
) -> List[str]:
    candidates: List[str] = []
    for f in all_files:
        dt = parse_ts_from_name(f, report)
        if dt is None:
            continue
        if gate_dt is not None and dt <= gate_dt:
            continue
        candidates.append(f)

    candidates.sort()
    return candidates[: max(0, limit)]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest AEMO dispatch ZIPs into raw inbox (watermark based).")
    p.add_argument("--limit", type=int, default=50, help="Max number of files to download (default 50)")
    p.add_argument("--dry-run", action="store_true", help="List actions without downloading")
    p.add_argument("--report", type=str, default=None, help=f"Report under CURRENT (default {DEFAULT_REPORT})")

    p.add_argument("--since", type=str, default=None, help='Gate (format "YYYYMMDDHHMM")')
    p.add_argument("--backfill-days", type=int, default=None, help="Backfill N days from now (example: 7)")
    p.add_argument(
        "--no-update-watermark",
        action="store_true",
        help="Do not update watermark after run (useful for backfills).",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    setup_logging()
    args = parse_args(argv)
    paths = resolve_paths()
    ensure_dirs(paths)

    report = get_report_name(args.report)
    base_url = report_url(report)

    watermark_dt = load_watermark(paths.watermark_file)
    try:
        gate_dt = effective_gate(watermark_dt, args.since, args.backfill_days)
    except ValueError:
        logging.error('Invalid time format. Use "YYYYMMDDHHMM" for --since.')
        return 2

    logging.info(f"Project root: {paths.project_root}")
    logging.info(f"Inbox dir: {paths.inbox_dir}")
    logging.info(f"Watermark file: {paths.watermark_file}")
    logging.info(f"Report: {report}")
    logging.info(f"Report URL: {base_url}")
    logging.info(f"Current watermark: {watermark_dt.strftime('%Y%m%d%H%M') if watermark_dt else 'None'}")
    if args.since:
        logging.info(f"Gate from --since: {gate_dt.strftime('%Y%m%d%H%M')}")
    elif args.backfill_days is not None:
        logging.info(f"Gate from --backfill-days {args.backfill_days}: {gate_dt.strftime('%Y%m%d%H%M')}")
    else:
        logging.info(f"Gate from watermark: {gate_dt.strftime('%Y%m%d%H%M') if gate_dt else 'None'}")

    if args.no_update_watermark:
        logging.info("Watermark updates disabled for this run (--no-update-watermark).")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "energy-intel-pipeline/1.0 (+ingestion)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    try:
        html = fetch_directory_listing(session, base_url)
        all_files = extract_zip_links(html)
        all_files = [f for f in all_files if REPORT_PATTERNS[report].match(f)]
        logging.info(f"Found {len(all_files)} matching ZIPs in listing")
    except Exception as e:
        logging.error(f"Failed to fetch/parse listing: {e}")
        return 2

    candidates = pick_candidates(all_files, report, gate_dt, args.limit)
    logging.info(f"Files to ingest this run: {len(candidates)}")

    if not candidates:
        logging.info("Nothing to do")
        return 0

    success = 0
    failed: List[str] = []
    newest_dt: Optional[datetime] = watermark_dt

    for name in candidates:
        try:
            download_zip(session, base_url, name, paths.inbox_dir, dry_run=args.dry_run)
            if not args.dry_run:
                dt = parse_ts_from_name(name, report)
                if dt is not None and (newest_dt is None or dt > newest_dt):
                    newest_dt = dt
            success += 1
        except Exception as e:
            logging.error(f"Failed to ingest {name}: {e}")
            failed.append(name)

    if (not args.dry_run) and (not args.no_update_watermark) and newest_dt is not None and newest_dt != watermark_dt:
        save_watermark(paths.watermark_file, newest_dt)
        logging.info(f"Watermark updated to: {newest_dt.strftime('%Y%m%d%H%M')}")

    logging.info(f"Ingestion complete. success={success} failed={len(failed)}")

    if failed:
        logging.error("Failed files:")
        for f in failed:
            logging.error(f"  - {f}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

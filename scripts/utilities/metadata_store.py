import hashlib
import os
from pathlib import Path
from typing import Optional, Tuple

from psycopg2.extras import RealDictCursor

from scripts.utilities.db import get_conn


def _sha256_file(path: Path) -> Tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _sha256_dir(path: Path) -> Tuple[str, int]:
    """
    Deterministic directory hash:
    - Walk files in sorted order
    - Include relative path bytes + file sha + file size
    """
    h = hashlib.sha256()
    total_size = 0

    base = path.resolve()
    files = sorted([p for p in base.rglob("*") if p.is_file()])

    for f in files:
        rel = str(f.relative_to(base)).replace("\\", "/").encode("utf-8")
        file_hash, file_size = _sha256_file(f)
        total_size += file_size

        h.update(rel)
        h.update(b"\x00")
        h.update(file_hash.encode("utf-8"))
        h.update(b"\x00")
        h.update(str(file_size).encode("utf-8"))
        h.update(b"\x00")

    if not files:
        h.update(b"EMPTY_DIR")

    return h.hexdigest(), total_size


def _hash_path(path: Path) -> Tuple[str, int]:
    if path.is_dir():
        return _sha256_dir(path)
    return _sha256_file(path)


class MetadataStore:
    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is not set")

    def start_run(
        self,
        command: str,
        raw_dir: Optional[str],
        max_files: Optional[int],
        cleanup: bool,
    ) -> str:
        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                insert into pipeline_run (command, status, rows_appended, started_at, raw_dir, max_files, cleanup)
                values (%s, 'running', 0, now(), %s, %s, %s)
                returning id
                """,
                (command, raw_dir, max_files, cleanup),
            )
            run_id = cur.fetchone()["id"]
            conn.commit()
            return str(run_id)

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_appended: int,
        error_message: Optional[str],
    ) -> None:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update pipeline_run
                set status = %s,
                    rows_appended = %s,
                    finished_at = now(),
                    error_message = %s
                where id = %s
                """,
                (status, rows_appended, error_message, run_id),
            )
            conn.commit()

    def register_raw_object(
        self,
        run_id: str,
        source_type: str,
        file_path: Path,
    ) -> str:
        sha, size = _hash_path(file_path)
        path_str = str(file_path.resolve())

        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Prefer schema with size_bytes
            try:
                cur.execute(
                    """
                    insert into raw_object (run_id, source_type, path, filename, size_bytes, sha256, discovered_at)
                    values (%s, %s, %s, %s, %s, %s, now())
                    on conflict (source_type, sha256) do update
                      set path = excluded.path,
                          filename = excluded.filename,
                          size_bytes = excluded.size_bytes,
                          discovered_at = excluded.discovered_at
                    returning id
                    """,
                    (run_id, source_type, path_str, file_path.name, size, sha),
                )
            except Exception:
                conn.rollback()
                cur.execute(
                    """
                    insert into raw_object (run_id, source_type, path, filename, sha256, discovered_at)
                    values (%s, %s, %s, %s, %s, now())
                    on conflict (source_type, sha256) do update
                      set path = excluded.path,
                          filename = excluded.filename,
                          discovered_at = excluded.discovered_at
                    returning id
                    """,
                    (run_id, source_type, path_str, file_path.name, sha),
                )

            rid = cur.fetchone()["id"]
            conn.commit()
            return str(rid)

    def register_artifact(
        self,
        run_id: str,
        artifact_type: str,
        path: Path,
    ) -> str:
        """
        File or directory artifacts only.
        This hashes the filesystem path, so it must exist.
        For DB artifacts, use register_db_artifact().
        """
        if not path.exists():
            raise FileNotFoundError(f"Artifact path does not exist: {path}")

        content_hash, size_bytes = _hash_path(path)
        path_str = str(path)

        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    insert into artifact (run_id, type, path, content_hash, size_bytes, created_at)
                    values (%s, %s, %s, %s, %s, now())
                    returning id
                    """,
                    (run_id, artifact_type, path_str, content_hash, size_bytes),
                )
            except Exception:
                conn.rollback()
                cur.execute(
                    """
                    insert into artifact (run_id, type, path, content_hash, created_at)
                    values (%s, %s, %s, %s, now())
                    returning id
                    """,
                    (run_id, artifact_type, path_str, content_hash),
                )

            artifact_id = cur.fetchone()["id"]
            conn.commit()
            return str(artifact_id)

    def register_db_artifact(
        self,
        run_id: str,
        artifact_type: str,
        table_name: str,
    ) -> str:
        """
        Record a database object as an artifact without filesystem hashing.
        Stores path like: db://public.dispatch_price_5min_region
        """
        path_str = f"db://{table_name}"

        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Do not set content_hash/size_bytes for DB objects
            cur.execute(
                """
                insert into artifact (run_id, type, path, content_hash, created_at)
                values (%s, %s, %s, null, now())
                returning id
                """,
                (run_id, artifact_type, path_str),
            )
            artifact_id = cur.fetchone()["id"]
            conn.commit()
            return str(artifact_id)

    def add_lineage(
        self,
        run_id: str,
        from_type: str,
        from_id: str,
        to_id: str,
    ) -> None:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into lineage_edge (run_id, from_type, from_id, to_type, to_id)
                values (%s, %s, %s, 'artifact', %s)
                on conflict do nothing
                """,
                (run_id, from_type, from_id, to_id),
            )
            conn.commit()

    def latest_run_id(self, command: str) -> Optional[str]:
        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                select id
                from pipeline_run
                where command = %s
                order by started_at desc
                limit 1
                """,
                (command,),
            )
            row = cur.fetchone()
            return str(row["id"]) if row else None

    def artifact_ids_for_run(self, run_id: str, artifact_type: str) -> list[str]:
        with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                select id
                from artifact
                where run_id = %s and type = %s
                order by path asc
                """,
                (run_id, artifact_type),
            )
            rows = cur.fetchall()
            return [str(r["id"]) for r in rows]

    def raw_object_exists_by_sha(self, source_type: str, sha256: str) -> bool:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select 1
                from raw_object
                where source_type = %s and sha256 = %s
                limit 1
                """,
                (source_type, sha256),
            )
            return cur.fetchone() is not None

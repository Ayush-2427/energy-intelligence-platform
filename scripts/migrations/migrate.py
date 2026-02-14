from pathlib import Path

from scripts.utilities.db import get_conn


def migrations_root() -> Path:
    # /app/scripts/migrations when running in container
    return Path(__file__).resolve().parent


def sql_dir() -> Path:
    # keep all SQL migrations here
    return migrations_root() / "sql"


def get_sql_files() -> list[Path]:
    d = sql_dir()
    if not d.exists():
        return []
    return sorted(d.glob("*.sql"))


def ensure_migrations_table() -> None:
    # IMPORTANT: use 'version' to match your existing DB
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migrations (
              version text primary key,
              applied_at timestamptz not null default now()
            );
            """
        )
        conn.commit()


def already_applied(version: str) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select 1 from schema_migrations where version = %s", (version,))
        return cur.fetchone() is not None


def apply_file(path: Path) -> None:
    sql = path.read_text(encoding="utf-8").strip()

    if not sql:
        print(f"Skipping empty migration: {path.name}")
        return

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "insert into schema_migrations(version) values (%s)",
            (path.name,),
        )
        conn.commit()



def main() -> None:
    ensure_migrations_table()

    files = get_sql_files()
    if not files:
        print("No migration files found. Nothing to do.")
        print(f"Expected folder: {sql_dir()}")
        return

    applied_any = False
    for f in files:
        if already_applied(f.name):
            continue
        apply_file(f)
        print(f"Applied migration: {f.name}")
        applied_any = True

    if not applied_any:
        print("Migrations complete. Nothing new to apply.")
    else:
        print("Migrations complete.")


if __name__ == "__main__":
    main()


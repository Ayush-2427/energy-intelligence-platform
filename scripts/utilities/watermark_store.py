from __future__ import annotations

from typing import Optional

from psycopg2.extras import RealDictCursor

from scripts.utilities.db import get_conn


def get_watermark(name: str) -> Optional[str]:
    """
    Returns watermark value (string) or None if not set.
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select value from watermark_state where name = %s", (name,))
        row = cur.fetchone()
        return row["value"] if row else None


def set_watermark(name: str, value: str) -> None:
    """
    Upserts watermark value.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into watermark_state (name, value, updated_at)
            values (%s, %s, now())
            on conflict (name)
            do update set value = excluded.value, updated_at = now()
            """,
            (name, value),
        )
        conn.commit()

# scripts/utilities/watermark_store.py
from __future__ import annotations

import os
from typing import Optional

import psycopg2


def _get_db_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def get_watermark(name: str) -> Optional[str]:
    """
    Reads watermark_state.value for a given watermark_state.name.
    Returns None if not set.
    """
    db_url = _get_db_url()
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select value
                from watermark_state
                where name = %s
                limit 1;
                """,
                (name,),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None


def set_watermark(name: str, value: str) -> None:
    """
    Upserts watermark_state(name, value) and bumps updated_at.
    """
    db_url = _get_db_url()
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into watermark_state (name, value, updated_at)
                values (%s, %s, now())
                on conflict (name)
                do update set value = excluded.value, updated_at = now();
                """,
                (name, str(value)),
            )
        conn.commit()

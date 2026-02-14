from __future__ import annotations

from typing import Dict, List, Tuple

import psycopg2.extras

from scripts.utilities.db import get_conn


def upsert_dispatch_prices(rows: List[Dict]) -> Tuple[int, int]:
    """
    Upserts dispatch price rows and returns (inserted_count, updated_count).

    Counting is done using RETURNING (xmax = 0) which is accurate for:
    - inserted rows: xmax = 0
    - conflict-updated rows: xmax != 0
    """
    if not rows:
        return 0, 0

    sql = """
        insert into dispatch_price (
            settlement_date,
            region_id,
            run_no,
            intervention,
            rrp
        )
        values %s
        on conflict (settlement_date, region_id, run_no, intervention)
        do update set
            rrp = excluded.rrp
        returning (xmax = 0) as inserted;
    """

    values = [
        (
            r["settlement_date"],
            r["region_id"],
            r["run_no"],
            r["intervention"],
            r["rrp"],
        )
        for r in rows
    ]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                sql,
                values,
                page_size=5000,
            )
            result = cur.fetchall()
            inserted = sum(1 for (flag,) in result if flag)
            updated = len(result) - inserted
        conn.commit()
        return inserted, updated
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

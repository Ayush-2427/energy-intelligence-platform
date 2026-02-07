# scripts/utilities/dispatch_price_repo.py
from __future__ import annotations

from typing import Iterable, Tuple

from psycopg2.extras import execute_values

from scripts.utilities.db import get_conn


# We mark whether row was inserted or updated:
# In Postgres, xmax = 0 typically means "inserted in this statement", otherwise it was updated.
# Important: the WHERE clause avoids pointless updates when rrp did not change.
UPSERT_RETURNING_SQL = """
insert into dispatch_price (settlement_date, region_id, rrp, intervention, run_no)
values %s
on conflict (settlement_date, region_id, run_no, intervention)
do update set
  rrp = excluded.rrp,
  ingested_at = now()
where dispatch_price.rrp is distinct from excluded.rrp
returning (xmax = 0) as inserted;
"""


def upsert_dispatch_prices(rows: Iterable[dict]) -> Tuple[int, int]:
    """
    Returns (inserted_count, updated_count)

    Notes:
      - If a row already exists and rrp is the same, no update happens.
      - If rrp differs, we update rrp and ingested_at.
    """
    rows_list = list(rows)
    if not rows_list:
        return (0, 0)

    payload = [
        (
            r["settlement_date"],
            r["region_id"],
            float(r["rrp"]),
            int(r.get("intervention", 0)),
            int(r.get("run_no", 0)),
        )
        for r in rows_list
    ]

    inserted = 0
    updated = 0

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_RETURNING_SQL, payload, page_size=5000)
        results = cur.fetchall()
        conn.commit()

    for (was_inserted,) in results:
        if was_inserted:
            inserted += 1
        else:
            updated += 1

    return (inserted, updated)

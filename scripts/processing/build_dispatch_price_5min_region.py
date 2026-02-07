from __future__ import annotations

import os
from typing import Optional

from scripts.utilities.db import get_conn
from scripts.utilities.metadata_store import MetadataStore
from pathlib import Path
UPSERT_SQL = """
insert into dispatch_price_5min_region (settlement_date, region_id, rrp, ingested_at)
select settlement_date, region_id, rrp, max(ingested_at) as ingested_at
from dispatch_price
where settlement_date >= %s
group by settlement_date, region_id, rrp
on conflict (settlement_date, region_id)
do update set
  rrp = excluded.rrp,
  ingested_at = excluded.ingested_at
"""

COUNT_SQL = "select count(*) from dispatch_price_5min_region where settlement_date >= %s"


def main(since: str = "1900-01-01 00:00:00") -> None:
    store: Optional[MetadataStore] = None
    run_id: Optional[str] = None

    try:
        if os.getenv("DATABASE_URL"):
            store = MetadataStore()
    except Exception as e:
        print(f"Metadata disabled (DB unavailable): {e}")
        store = None

    if store:
        try:
            run_id = store.start_run(command="curate_5min", raw_dir=None, max_files=None, cleanup=False)
        except Exception as e:
            print(f"Metadata disabled (failed to start run): {e}")
            store = None
            run_id = None

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, (since,))
        conn.commit()

        cur.execute(COUNT_SQL, (since,))
        n = cur.fetchone()[0]

    if store and run_id:
        try:
            artifact_id = store.register_db_artifact(
                run_id,
                "curated_table",
                "public.dispatch_price_5min_region",
            )

            # lineage from source table not represented as artifact yet, so skip or add later
        except Exception as e:
            print(f"Metadata warn: could not record curated artifact: {e}")

        store.finish_run(run_id, status="success", rows_appended=int(n), error_message=None)

    print(f"Curated rows (>= {since}): {n}")


if __name__ == "__main__":
    main()

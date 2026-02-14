from scripts.utilities.db import get_conn


def populate_daily_summary():
    sql = """
    insert into daily_region_summary (
        region_id,
        date_id,
        avg_rrp,
        max_rrp,
        min_rrp,
        price_stddev,
        interval_count
    )
    select
        df.region_id,
        df.date_id,
        avg(df.rrp),
        max(df.rrp),
        min(df.rrp),
        stddev(df.rrp),
        count(*)
    from dispatch_fact df
    group by df.region_id, df.date_id
    on conflict do nothing;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def main():
    print("Building daily_region_summary...")
    populate_daily_summary()
    print("Done.")


if __name__ == "__main__":
    main()

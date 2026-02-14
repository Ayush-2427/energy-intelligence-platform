from scripts.utilities.db import get_conn


def populate_dimension_date():
    """
    Insert distinct dates from dispatch_price into dimension_date.
    """
    sql = """
    insert into dimension_date (full_date, day, month, year, quarter, week_of_year, is_weekend, season)
    select distinct
        date(settlement_date) as full_date,
        extract(day from settlement_date)::int,
        extract(month from settlement_date)::int,
        extract(year from settlement_date)::int,
        extract(quarter from settlement_date)::int,
        extract(week from settlement_date)::int,
        extract(isodow from settlement_date) in (6,7) as is_weekend,
        case
            when extract(month from settlement_date) in (12,1,2) then 'Summer'
            when extract(month from settlement_date) in (3,4,5) then 'Autumn'
            when extract(month from settlement_date) in (6,7,8) then 'Winter'
            else 'Spring'
        end as season
    from dispatch_price dp
    where not exists (
        select 1 from dimension_date dd
        where dd.full_date = date(dp.settlement_date)
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def populate_dispatch_fact():
    """
    Populate dispatch_fact from dispatch_price.
    """
    sql = """
    insert into dispatch_fact (
        date_id,
        region_id,
        interval_timestamp,
        rrp,
        intervention,
        run_no
    )
    select
        dd.date_id,
        dr.region_id,
        dp.settlement_date,
        dp.rrp,
        dp.intervention,
        dp.run_no
    from dispatch_price dp
    join dimension_date dd
        on dd.full_date = date(dp.settlement_date)
    join dimension_region dr
        on dr.region_code = dp.region_id
        and dr.is_current = true
    on conflict do nothing;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def main():
    print("Populating dimension_date...")
    populate_dimension_date()

    print("Populating dispatch_fact...")
    populate_dispatch_fact()

    print("Done.")


if __name__ == "__main__":
    main()

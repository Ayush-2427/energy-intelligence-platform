
create table if not exists daily_region_summary (
    summary_id bigserial primary key,
    region_id int not null references dimension_region(region_id),
    date_id int not null references dimension_date(date_id),

    avg_rrp double precision,
    max_rrp double precision,
    min_rrp double precision,
    price_stddev double precision,

    interval_count int,

    created_at timestamptz not null default now(),

    unique (region_id, date_id)
);

create index if not exists idx_daily_region_summary_region_date
    on daily_region_summary(region_id, date_id);

create table if not exists rolling_metrics (
    rolling_id bigserial primary key,

    region_id int not null references dimension_region(region_id),
    date_id int not null references dimension_date(date_id),

    rolling_7d_avg_price double precision,
    rolling_7d_avg_stddev double precision,

    created_at timestamptz not null default now(),

    unique (region_id, date_id)
);

create index if not exists idx_rolling_metrics_region_date
    on rolling_metrics(region_id, date_id);

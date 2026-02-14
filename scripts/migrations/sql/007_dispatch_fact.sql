
create table if not exists dispatch_fact (
    fact_id bigserial primary key,

    date_id int not null references dimension_date(date_id),
    region_id int not null references dimension_region(region_id),

    interval_timestamp timestamptz not null,

    rrp double precision not null,
    intervention integer not null default 0,
    run_no integer not null default 0,

    created_at timestamptz not null default now(),

    unique (interval_timestamp, region_id, run_no, intervention)
);

create index if not exists idx_dispatch_fact_region_time
    on dispatch_fact(region_id, interval_timestamp);

create index if not exists idx_dispatch_fact_date
    on dispatch_fact(date_id);

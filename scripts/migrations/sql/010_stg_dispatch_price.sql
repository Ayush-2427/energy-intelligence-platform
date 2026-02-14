
create table if not exists stg_dispatch_price (
    settlement_date timestamp not null,
    region_id text not null,
    rrp numeric not null,
    intervention integer not null,
    run_no integer not null,

    source_file text not null,
    loaded_at timestamp not null default now(),

    primary key (settlement_date, region_id, run_no, intervention, source_file)
);

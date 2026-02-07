-- 004_dispatch_price_5min_region.sql

create table if not exists dispatch_price_5min_region (
  settlement_date timestamp without time zone not null,
  region_id text not null,
  rrp double precision not null,
  ingested_at timestamptz not null default now(),
  primary key (settlement_date, region_id)
);

-- Helpful indexes for common queries
create index if not exists idx_dp5_region_date on dispatch_price_5min_region (region_id, settlement_date);
create index if not exists idx_dp5_date on dispatch_price_5min_region (settlement_date);

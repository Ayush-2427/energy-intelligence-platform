-- 002_dispatch_price_fact.sql
-- Fact table to store dispatch prices with idempotent upserts.

create table if not exists dispatch_price (
  settlement_date timestamp not null,
  region_id text not null,
  rrp double precision not null,
  intervention integer not null default 0,
  run_no integer not null default 0,
  ingested_at timestamptz not null default now(),

  primary key (settlement_date, region_id, run_no, intervention)
);

create index if not exists idx_dispatch_price_region_settlement
  on dispatch_price (region_id, settlement_date);

create index if not exists idx_dispatch_price_settlement
  on dispatch_price (settlement_date);

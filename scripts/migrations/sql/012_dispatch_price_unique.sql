begin;

-- Enforce the natural key for dispatch prices
create unique index if not exists uq_dispatch_price_key
on dispatch_price (settlement_date, region_id, run_no, intervention);

commit;

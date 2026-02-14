-- 005_dimensions.sql
-- Introduce warehouse dimension tables
-- Does NOT modify existing dispatch_price table

-- ==============================
-- Dimension: Date
-- ==============================

create table if not exists dimension_date (
    date_id serial primary key,
    full_date date not null unique,
    day int not null,
    month int not null,
    year int not null,
    quarter int not null,
    week_of_year int not null,
    is_weekend boolean not null,
    season varchar(20) not null
);

create index if not exists idx_dimension_date_full_date
    on dimension_date(full_date);


-- ==============================
-- Dimension: Region
-- ==============================

create table if not exists dimension_region (
    region_id serial primary key,
    region_code text not null,
    state text not null,
    timezone text not null default 'Australia/Sydney',

    valid_from timestamptz not null default now(),
    valid_to timestamptz,
    is_current boolean not null default true,

    unique (region_code, is_current)
);

create index if not exists idx_dimension_region_code
    on dimension_region(region_code);


-- ==============================
-- Dimension: Generation Type
-- ==============================

create table if not exists dimension_generation_type (
    gen_type_id serial primary key,
    gen_type_name text not null unique
);

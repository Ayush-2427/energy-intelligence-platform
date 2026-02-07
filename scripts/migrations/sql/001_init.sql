create extension if not exists pgcrypto;

create table if not exists pipeline_run (
  id uuid primary key default gen_random_uuid(),
  command text not null,
  raw_dir text,
  max_files int,
  cleanup boolean default false,
  status text not null default 'running',
  rows_appended int default 0,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  error_message text
);

create table if not exists raw_object (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references pipeline_run(id) on delete cascade,
  source_type text not null, -- csv, zip
  path text not null,
  filename text not null,
  size_bytes bigint,
  sha256 text,
  discovered_at timestamptz not null default now()
);

create table if not exists artifact (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references pipeline_run(id) on delete cascade,
  type text not null, -- clean_csv, parquet_partition, rollup
  path text not null,
  content_hash text,
  created_at timestamptz not null default now()
);

create table if not exists lineage_edge (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references pipeline_run(id) on delete cascade,
  from_type text not null, -- raw_object, artifact
  from_id uuid not null,
  to_type text not null default 'artifact',
  to_id uuid not null
);

create index if not exists idx_raw_object_run on raw_object(run_id);
create index if not exists idx_artifact_run on artifact(run_id);
create index if not exists idx_lineage_run on lineage_edge(run_id);

create table if not exists watermark_state (
  name text primary key,
  value text,
  updated_at timestamptz not null default now()
);

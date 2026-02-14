create table if not exists pipeline_run_metrics (
    id bigserial primary key,
    run_type text not null,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    status text not null default 'running',

    total_files integer default 0,
    total_rows integer default 0,
    total_inserted integer default 0,
    total_updated integer default 0,
    total_invalid integer default 0,

    error_message text
);

create index if not exists idx_pipeline_run_metrics_started
    on pipeline_run_metrics(started_at);

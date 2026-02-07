-- Prevent re-registering the same exact file content for the same source type
-- sha256 can be null in some older rows, so we only enforce uniqueness when sha256 is present.

create unique index if not exists uq_raw_object_source_sha
on raw_object (source_type, sha256)
where sha256 is not null;

-- Optional but useful for lookups
create index if not exists idx_raw_object_source_type_sha
on raw_object (source_type, sha256);

create index if not exists idx_raw_object_path
on raw_object (path);

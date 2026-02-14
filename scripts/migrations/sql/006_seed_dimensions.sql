-- 006_seed_dimensions.sql
-- Seed static reference data

-- Regions (current NEM regions)
insert into dimension_region (region_code, state, timezone)
values
  ('NSW1', 'NSW', 'Australia/Sydney'),
  ('QLD1', 'QLD', 'Australia/Brisbane'),
  ('SA1',  'SA',  'Australia/Adelaide'),
  ('TAS1', 'TAS', 'Australia/Hobart'),
  ('VIC1', 'VIC', 'Australia/Melbourne')
on conflict do nothing;


-- Generation types
insert into dimension_generation_type (gen_type_name)
values
  ('Coal'),
  ('Gas'),
  ('Hydro'),
  ('Wind'),
  ('Solar'),
  ('Battery')
on conflict do nothing;


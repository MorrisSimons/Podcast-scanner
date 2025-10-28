-- Truncate all user tables in the public schema, restart identities, and cascade
do $$
declare
  stmt text;
begin
  select 'truncate table ' || string_agg(format('%I.%I', schemaname, tablename), ', ')
         || ' restart identity cascade'
    into stmt
  from pg_tables
  where schemaname = 'public'
    and tablename not like 'pg_%';

  if stmt is not null then
    execute stmt;
  end if;
end $$;



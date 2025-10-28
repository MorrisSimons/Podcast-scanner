with checks as (

  -- 001_enable_pgcrypto.sql
  select 'pgcrypto extension installed' as label, exists (
    select 1 from pg_extension e where e.extname = 'pgcrypto'
  ) as ok, null::text as details

  union all
  -- 002_create_podcasts.sql
  select 'table public.podcasts exists', to_regclass('public.podcasts') is not null, null
  union all
  select 'podcasts.id default gen_random_uuid()', exists (
    select 1
    from information_schema.columns c
    where c.table_schema='public' and c.table_name='podcasts'
      and c.column_name='id'
      and c.column_default ilike '%gen_random_uuid()%'
  ), null
  union all
  select 'podcasts.updated_at default now()', exists (
    select 1
    from information_schema.columns c
    where c.table_schema='public' and c.table_name='podcasts'
      and c.column_name='updated_at'
      and c.column_default ilike '%now()%'
  ), null
  union all
  select 'podcasts.rss_feed_url UNIQUE', exists (
    select 1
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name=kcu.constraint_name
     and tc.table_schema=kcu.table_schema
     and tc.table_name=kcu.table_name
    where tc.table_schema='public'
      and tc.table_name='podcasts'
      and tc.constraint_type='UNIQUE'
      and kcu.column_name='rss_feed_url'
  ), null
  union all
  select 'trigger podcasts_set_updated_at exists', exists (
    select 1
    from pg_trigger tg
    join pg_class t on t.oid=tg.tgrelid
    join pg_proc p on p.oid=tg.tgfoid
    where t.relname='podcasts'
      and tg.tgname='podcasts_set_updated_at'
      and tg.tgenabled <> 'D'
      and p.proname='set_updated_at'
  ), null

  union all
  -- 003_create_episodes.sql
  select 'table public.episodes exists', to_regclass('public.episodes') is not null, null
  union all
  select 'episodes FK podcast_id → podcasts(id) CASCADE', exists (
    select 1
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
    join information_schema.referential_constraints rc
      on rc.constraint_name = tc.constraint_name
     and rc.constraint_schema = tc.table_schema
    join information_schema.constraint_column_usage ccu
      on ccu.constraint_name = tc.constraint_name
     and ccu.constraint_schema = tc.table_schema
    where tc.table_schema='public'
      and tc.table_name='episodes'
      and tc.constraint_type='FOREIGN KEY'
      and kcu.column_name='podcast_id'
      and ccu.table_name='podcasts'
      and ccu.column_name='id'
      and rc.delete_rule='CASCADE'
  ), null
  union all
  select 'episodes UNIQUE (podcast_id, guid)', exists (
    with u as (
      select tc.constraint_name,
             string_agg(kcu.column_name, ',' order by kcu.ordinal_position) as cols
      from information_schema.table_constraints tc
      join information_schema.key_column_usage kcu
        on tc.constraint_name=kcu.constraint_name
       and tc.table_schema=kcu.table_schema
       and tc.table_name=kcu.table_name
      where tc.table_schema='public'
        and tc.table_name='episodes'
        and tc.constraint_type='UNIQUE'
      group by tc.constraint_name
    )
    select 1 from u where cols='podcast_id,guid'
  ), null
  union all
  select 'trigger episodes_set_updated_at exists', exists (
    select 1
    from pg_trigger tg
    join pg_class t on t.oid=tg.tgrelid
    join pg_proc p on p.oid=tg.tgfoid
    where t.relname='episodes'
      and tg.tgname='episodes_set_updated_at'
      and tg.tgenabled <> 'D'
      and p.proname='set_updated_at'
  ), null

  union all
  -- 004_add_indexes.sql
  select 'index episodes_podcast_pubdate_idx exists', to_regclass('public.episodes_podcast_pubdate_idx') is not null, null
  union all
  select 'index episodes_guid_idx exists', to_regclass('public.episodes_guid_idx') is not null, null
  union all
  select 'index episodes_audio_url_idx exists', to_regclass('public.episodes_audio_url_idx') is not null, null
  union all
  select 'unique partial index episodes_podcast_audio_unique exists', exists (
    select 1 from pg_indexes
    where schemaname='public' and tablename='episodes'
      and indexname='episodes_podcast_audio_unique'
      and indexdef ~* 'unique' and indexdef ~* '\\(podcast_id,\\s*audio_url\\)'
      and indexdef ~* 'where \\(audio_url is not null\\)'
  ), null
)
select
  label,
  ok,
  case when ok then 'ok' else 'missing or invalid' end as details -- TODO: Might be a error here, but probably okay.
from checks
order by label;
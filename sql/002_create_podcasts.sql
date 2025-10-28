begin;

-- Podcasts table (one row per podcast)
create table if not exists public.podcasts (
  id uuid primary key default gen_random_uuid(),
  rss_feed_url text not null unique,
  title text not null,
  description text,
  website_url text,              -- <link>
  image_url text,                -- <image><url> or <itunes:image href="...">
  language text,                 -- <language>
  categories text[],             -- multiple <itunes:category>
  author text,                   -- <itunes:author>
  explicit boolean,              -- <itunes:explicit>
  itunes_owner_name text,        -- <itunes:owner><itunes:name>
  itunes_owner_email text,       -- <itunes:owner><itunes:email>
  itunes_podcast_guid text,      -- <itunes:podcastGuid>
  source text,                   -- provider/file this came from (e.g., "acast")
  last_build_at timestamptz,     -- <lastBuildDate>
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Keep updated_at fresh
create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end; $$;

do $$
begin
  if not exists (select 1 from pg_trigger where tgname='podcasts_set_updated_at') then
    create trigger podcasts_set_updated_at
    before update on public.podcasts
    for each row execute function public.set_updated_at();
  end if;
end $$;

-- Ensure unique constraint exists even if table pre-existed without it
do $$
begin
  if not exists (
    select 1 from pg_indexes
    where schemaname='public' and tablename='podcasts'
      and indexdef ~* 'unique.*\(rss_feed_url\)'
  ) then
    alter table public.podcasts
      add constraint podcasts_rss_feed_url_unique unique (rss_feed_url);
  end if;
end $$;

commit;

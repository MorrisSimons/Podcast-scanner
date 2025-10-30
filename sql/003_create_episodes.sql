begin;

-- Episodes table (one row per episode)
create table if not exists public.episodes (
  id uuid primary key default gen_random_uuid(),
  podcast_id uuid not null references public.podcasts(id) on delete cascade,
  guid text not null,                   -- <guid> (may be permalink=false)
  title text not null,
  description text,                     -- <description>
  content_html text,                    -- <content:encoded>
  pub_date timestamptz,                 -- <pubDate>
  duration_seconds integer,             -- <itunes:duration> parsed to seconds
  episode_number integer,               -- <itunes:episode>
  season_number integer,                -- <itunes:season>
  episode_type text,                    -- <itunes:episodeType>: full|trailer|bonus
  explicit boolean,                     -- <itunes:explicit>
  audio_url text,                       -- <enclosure url>
  audio_type text,                      -- <enclosure type>
  audio_length_bytes bigint,            -- <enclosure length>
  image_url text,                       -- <itunes:image href>
  link_url text,                        -- <link>
  keywords text[],                      -- <itunes:keywords> split by comma
  transcript_url text,                  -- e.g., <podcast:transcript url>
  chapters jsonb,                       -- e.g., parsed from <podcast:chapters url>
  source text,                          -- provider/file this came from
  status_code integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (podcast_id, guid)
);

-- Ensure unique constraint exists even if table pre-existed without it
do $$
begin
  if not exists (
    select 1 from pg_indexes
    where schemaname='public' and tablename='episodes'
      and indexdef ~* 'unique.*\(podcast_id,\s*guid\)'
  ) then
    alter table public.episodes
      add constraint episodes_podcast_guid_unique unique (podcast_id, guid);
  end if;
end $$;

do $$
begin
  if not exists (select 1 from pg_trigger where tgname='episodes_set_updated_at') then
    create trigger episodes_set_updated_at
    before update on public.episodes
    for each row execute function public.set_updated_at();
  end if;
end $$;

commit;

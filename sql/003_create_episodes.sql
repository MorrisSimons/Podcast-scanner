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
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  mp3_download_status boolean,          -- true when audio uploaded to S3
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

-- Unique index on podcast_id and audio_url where audio_url is not null
create unique index if not exists episodes_podcast_audio_unique on public.episodes (podcast_id, audio_url)
where audio_url is not null;

-- Index on podcast_id and pub_date for efficient episode queries
create index if not exists episodes_podcast_pubdate_idx on public.episodes (podcast_id, pub_date desc);

-- Index on guid for fast lookups
create index if not exists episodes_guid_idx on public.episodes (guid);

-- Index on audio_url for S3 upload queries
create index if not exists episodes_audio_url_idx on public.episodes (audio_url);

-- Index on mp3_download_status for filtering undownloaded episodes
create index if not exists episodes_mp3_download_status_idx on public.episodes (mp3_download_status);

do $$
begin
  if not exists (select 1 from pg_trigger where tgname='episodes_set_updated_at') then
    create trigger episodes_set_updated_at
    before update on public.episodes
    for each row execute function public.set_updated_at();
  end if;
end $$;

commit;

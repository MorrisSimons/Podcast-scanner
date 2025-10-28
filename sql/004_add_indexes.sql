-- Helpful indexes for episode lookups and de-duplication
create index if not exists episodes_podcast_pubdate_idx on public.episodes (podcast_id, pub_date desc);
create index if not exists episodes_guid_idx on public.episodes (guid);
create index if not exists episodes_audio_url_idx on public.episodes (audio_url);

-- Optional: prevent duplicate audio within a podcast when present
create unique index if not exists episodes_podcast_audio_unique
  on public.episodes (podcast_id, audio_url)
  where audio_url is not null;

begin;

-- Replace podcast_profiles table with explicit columns and status_code
 drop table if exists public.podcast_profiles;

 create table public.podcast_profiles (
   id text primary key,
   rss_feed_url text,
   podcast_name text,
   supplier_id text,
   supplier_name text,
   network_id text,
   network_name text,
   genre text,
   status_code integer,
   created_at timestamptz not null default now(),
   updated_at timestamptz not null default now()
 );

 -- Ensure set_updated_at() exists
 create or replace function public.set_updated_at()
 returns trigger language plpgsql as $$
 begin
   new.updated_at := now();
   return new;
 end; $$;

 -- Keep updated_at fresh
 do $$
 begin
   if not exists (select 1 from pg_trigger where tgname='podcast_profiles_set_updated_at') then
     create trigger podcast_profiles_set_updated_at
     before update on public.podcast_profiles
     for each row execute function public.set_updated_at();
   end if;
 end $$;

 commit;



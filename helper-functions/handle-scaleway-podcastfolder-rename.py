#!/usr/bin/env python3
import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()





class SupabaseRestClient:
	def __init__(self, base_url: str, service_role_key: str) -> None:
		self.base_url = base_url.rstrip("/") + "/rest/v1"
		self.api_key = service_role_key
		self.session = requests.Session()
		self.default_headers = {
			"apikey": self.api_key,
			"Authorization": f"Bearer {self.api_key}",
			"Content-Type": "application/json",
		}

	def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
		url = self.base_url + path
		return self.session.get(url, params=params or {}, headers=self.default_headers)


def normalize_rss_url(url: Optional[str]) -> Optional[str]:
	if not url:
		return None
	u = url.strip()
	u = re.sub(r"/+$", "", u)
	return u.lower()


def extract_filename_from_url(url: str) -> Optional[str]:
	try:
		from urllib.parse import urlparse
		parsed = urlparse(url)
		path = parsed.path or ""
		filename = os.path.basename(path)
		if filename:
			return filename
	except Exception:
		pass

	stripped = url.split("?", 1)[0]
	if "/" in stripped:
		cand = stripped.rstrip("/").rsplit("/", 1)[-1]
		return cand if cand else None
	return None


@dataclass
class EpisodeRow:
	episode_id: str
	podcast_id: str
	audio_url: str


@dataclass
class MappingResult:
	episode_id: str
	podcast_id: str
	external_podcast_id: Optional[str]
	audio_url: str
	old_key: Optional[str]
	new_key: Optional[str]
	status: str


def fetch_reference_maps(client: SupabaseRestClient) -> Tuple[Dict[str, str], Dict[str, str]]:
	podcasts_resp = client.get("/podcasts", params={"select": "id,rss_feed_url"})
	if podcasts_resp.status_code != 200:
		raise RuntimeError(f"Failed to fetch podcasts: HTTP {podcasts_resp.status_code} - {podcasts_resp.text}")
	podcasts = podcasts_resp.json()

	profiles_resp = client.get("/podcast_profiles", params={"select": "id,rss_feed_url"})
	if profiles_resp.status_code != 200:
		raise RuntimeError(f"Failed to fetch podcast_profiles: HTTP {profiles_resp.status_code} - {profiles_resp.text}")
	profiles = profiles_resp.json()

	podcast_id_to_rss: Dict[str, str] = {}
	for r in podcasts:
		if r.get("id") and r.get("rss_feed_url"):
			podcast_id_to_rss[str(r["id"])] = str(r["rss_feed_url"])

	rss_to_external_id: Dict[str, str] = {}
	for r in profiles:
		if r.get("id") and r.get("rss_feed_url"):
			rss_to_external_id[normalize_rss_url(str(r["rss_feed_url"]))] = str(r["id"])

	return podcast_id_to_rss, rss_to_external_id


def fetch_episode_sample(client: SupabaseRestClient, limit: int) -> List[EpisodeRow]:
	resp = client.get(
		"/episodes",
		params={
			"select": "id,podcast_id,audio_url",
			"mp3_download_status": "is.true",
			"audio_url": "is.not_null",
			"limit": limit,
		},
	)
	if resp.status_code != 200:
		raise RuntimeError(f"Failed to fetch episodes: HTTP {resp.status_code} - {resp.text}")
	rows = resp.json()
	out: List[EpisodeRow] = []
	for r in rows:
		out.append(
			EpisodeRow(
				episode_id=str(r["id"]),
				podcast_id=str(r["podcast_id"]),
				audio_url=str(r["audio_url"]),
			)
		)
	return out


def build_mappings(
	episodes: List[EpisodeRow],
	podcast_id_to_rss: Dict[str, str],
	rss_to_external_id: Dict[str, str],
) -> List[MappingResult]:
	results: List[MappingResult] = []
	for e in episodes:
		status = "OK"

		rss_url = podcast_id_to_rss.get(e.podcast_id)
		external_id = None
		if rss_url:
			external_id = rss_to_external_id.get(normalize_rss_url(rss_url))
		if not external_id:
			status = "MISSING_PROFILE"

		filename = extract_filename_from_url(e.audio_url)
		if not filename:
			status = "BAD_URL"

		old_key = f"{e.podcast_id}/{filename}" if filename else None
		new_key = f"{e.podcast_id}/{e.episode_id}.mp3"

		results.append(
			MappingResult(
				episode_id=e.episode_id,
				podcast_id=e.podcast_id,
				external_podcast_id=external_id,
				audio_url=e.audio_url,
				old_key=old_key,
				new_key=new_key,
				status=status,
			)
		)
	return results


def main() -> None:
	parser = argparse.ArgumentParser(description="Validate mapping from old Scaleway keys to new scheme.")
	parser.add_argument("--limit", type=int, default=1000, help="Number of episodes to sample")
	args = parser.parse_args()

	try:
		SUPABASE_URL = require_env("SUPABASE_URL")
		SUPABASE_SERVICE_ROLE_KEY = require_env("SUPABASE_SERVICE_ROLE_KEY")
	except Exception as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		sys.exit(2)

	client = SupabaseRestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

	try:
		podcast_id_to_rss, rss_to_external_id = fetch_reference_maps(client)
		episodes = fetch_episode_sample(client, args.limit)
	except Exception as exc:
		print(f"ERROR: {exc}", file=sys.stderr)
		sys.exit(2)

	results = build_mappings(episodes, podcast_id_to_rss, rss_to_external_id)

	# Group by external_podcast_id to show current Scaleway structure
	by_external_id: Dict[str, List[MappingResult]] = {}
	for r in results:
		if r.external_podcast_id:
			by_external_id.setdefault(r.external_podcast_id, []).append(r)

	print("=" * 80)
	print("CURRENT SCALEWAY STRUCTURE (by external podcast_id)")
	print("=" * 80)
	for ext_id in sorted(by_external_id.keys())[:10]:  # Show first 10 external IDs
		items = by_external_id[ext_id]
		print(f"\n{ext_id}/ ({len(items)} files)")
		for r in items[:3]:  # Show first 3 files per folder
			filename = r.old_key.split("/", 1)[1] if r.old_key else "?"
			print(f"  {filename}")
		if len(items) > 3:
			print(f"  ... and {len(items) - 3} more")

	# Summary
	counts: Dict[str, int] = {}
	for r in results:
		counts[r.status] = counts.get(r.status, 0) + 1

	print("\n" + "=" * 80)
	print("MAPPING VALIDATION SUMMARY")
	print("=" * 80)
	for status in sorted(counts.keys()):
		print(f"{status}: {counts[status]}")

	# Print all valid mappings
	ok_results = [r for r in results if r.status == "OK"]
	print("\n" + "=" * 80)
	print(f"MIGRATION PLAN ({len(ok_results)} files to migrate)")
	print("=" * 80)
	for r in ok_results:
		print(f"{r.old_key} -> {r.new_key}")


if __name__ == "__main__":
	main()



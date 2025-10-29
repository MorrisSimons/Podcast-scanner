import os
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests






# -----------------------------
# Environment helpers
# -----------------------------

def load_env_file_if_present(project_root: Path) -> None:
	"""Load .env from project root into os.environ if keys not already set."""
	dotenv_path = project_root / ".env"
	if not dotenv_path.exists():
		return
	try:
		with dotenv_path.open("r", encoding="utf-8") as f:
			for line in f:
				line = line.strip()
				if not line or line.startswith("#"):
					continue
				if "=" not in line:
					continue
				key, value = line.split("=", 1)
				key = key.strip()
				value = value.strip().strip('"').strip("'")
				if key and key not in os.environ:
					os.environ[key] = value
	except Exception as exc:
		raise RuntimeError(f"Failed to parse .env: {exc}")


def require_env(key: str) -> str:
	value = os.getenv(key)
	if not value:
		raise RuntimeError(f"Missing required env var: {key}")
	return value


# -----------------------------
# XML parsing helpers
# -----------------------------

def get_first_child_text(node: ET.Element, tag_suffix: str) -> Optional[str]:
	"""Return .text of the first child whose tag ends with the given suffix."""
	for child in list(node):
		if child.tag.endswith(tag_suffix):
			text = child.text.strip() if child.text else None
			return text if text else None
	return None


def get_first_descendant_text(node: ET.Element, tag_suffix: str) -> Optional[str]:
	"""Depth-first search for a descendant whose tag endswith tag_suffix, return its text."""
	for elem in node.iter():
		if elem.tag.endswith(tag_suffix):
			text = elem.text.strip() if elem.text else None
			if text:
				return text
	return None


def get_first_child_attr(node: ET.Element, tag_suffix: str, attr: str) -> Optional[str]:
	for child in list(node):
		if child.tag.endswith(tag_suffix):
			val = child.attrib.get(attr)
			if val is not None:
				val = val.strip()
				return val if val else None
	return None


def get_first_descendant_attr(node: ET.Element, tag_suffix: str, attr: str) -> Optional[str]:
	for elem in node.iter():
		if elem.tag.endswith(tag_suffix):
			val = elem.attrib.get(attr)
			if val is not None and str(val).strip():
				return str(val).strip()
	return None


def get_all_descendants(node: ET.Element, tag_suffix: str) -> List[ET.Element]:
	return [e for e in node.iter() if e.tag.endswith(tag_suffix)]


def parse_bool(text: Optional[str]) -> Optional[bool]:
	if text is None:
		return None
	val = text.strip().lower()
	if val in ("yes", "true", "explicit", "y", "1"):
		return True
	if val in ("no", "false", "clean", "n", "0"):
		return False
	return None


def parse_int(text: Optional[str]) -> Optional[int]:
	if text is None:
		return None
	try:
		return int(text.strip())
	except Exception:
		return None


def parse_duration_to_seconds(text: Optional[str]) -> Optional[int]:
	"""Parse itunes:duration which may be seconds or HH:MM:SS / MM:SS."""
	if text is None:
		return None
	val = text.strip()
	if not val:
		return None
	if val.isdigit():
		try:
			return int(val)
		except Exception:
			return None
	parts = val.split(":")
	try:
		parts = [int(p) for p in parts]
	except Exception:
		return None
	if len(parts) == 3:
		h, m, s = parts
		return h * 3600 + m * 60 + s
	if len(parts) == 2:
		m, s = parts
		return m * 60 + s
	return None


def parse_rfc2822_datetime(text: Optional[str]) -> Optional[str]:
	if text is None:
		return None
	try:
		dt = parsedate_to_datetime(text)
		# Convert to ISO8601 string acceptable by PostgREST
		return dt.isoformat()
	except Exception:
		return None


# -----------------------------
# Supabase REST client
# -----------------------------

class SupabaseRestClient:
	def __init__(self, base_url: str, service_role_key: str) -> None:
		self.base_url = base_url.rstrip("/") + "/rest/v1"
		self.api_key = service_role_key
		self.session = requests.Session()
		self.default_headers = {
			"apikey": self.api_key,
			"Authorization": f"Bearer {self.api_key}",
			"Content-Type": "application/json",
			"Prefer": "return=representation"
		}

	def post(self, path: str, payload: Any, params: Optional[Dict[str, Any]] = None, prefer: Optional[str] = None) -> requests.Response:
		url = self.base_url + path
		headers = dict(self.default_headers)
		if prefer:
			headers["Prefer"] = prefer
		return self.session.post(url, params=params or {}, headers=headers, data=json.dumps(payload))

	def rpc(self, function_name: str, args: Dict[str, Any]) -> requests.Response:
		url = self.base_url + f"/rpc/{function_name}"
		return self.session.post(url, headers=self.default_headers, data=json.dumps(args))


# -----------------------------
# ETL core
# -----------------------------

def derive_profile_basename_from_xml(xml_file: Path) -> str:
	# Expect filenames like "0a1bdbfb-2843-4b8d-8548-08dcd1d9d367_rss.xml" -> UUID "0a1bdbfb-2843-4b8d-8548-08dcd1d9d367"
	stem = xml_file.stem  # e.g., "0a1bdbfb-2843-4b8d-8548-08dcd1d9d367_rss"
	if stem.endswith("_rss"):
		return stem[:-4]
	return stem


def read_rss_feed_url_from_db(client: SupabaseRestClient, podcast_id: str) -> Tuple[Optional[str], Optional[str]]:
	"""Query podcast_profiles table for rss_feed_url and supplier_name using the podcast ID."""
	resp = client.post(
		path="/podcast_profiles",
		payload={},
		params={"id": f"eq.{podcast_id}", "select": "rss_feed_url,supplier_name"}
	)
	if resp.status_code != 200:
		# Return None values if lookup fails; podcast_profiles may not be populated yet
		return None, None
	data = resp.json()
	if not data or len(data) == 0:
		# Return None values if podcast profile not found
		return None, None
	record = data[0]
	rss_feed_url = record.get("rss_feed_url")
	supplier_name = record.get("supplier_name")
	return rss_feed_url, supplier_name


def parse_podcast_from_channel(channel: ET.Element, rss_feed_url: str, source: str) -> Dict[str, Any]:
	title = get_first_child_text(channel, "title") or ""
	if not title:
		raise ValueError("Channel missing required <title>")
	description = get_first_child_text(channel, "description")
	website_url = get_first_child_text(channel, "link")
	image_url = None
	# <image><url> or <itunes:image href="...">
	image_url = get_first_descendant_text(channel, "url") or get_first_descendant_attr(channel, "image", "href")
	language = get_first_child_text(channel, "language")
	author = get_first_descendant_text(channel, "author")
	explicit = parse_bool(get_first_descendant_text(channel, "explicit"))
	owner_name = None
	owner_email = None
	for owner in get_all_descendants(channel, "owner"):
		name = get_first_child_text(owner, "name")
		email = get_first_child_text(owner, "email")
		if name:
			owner_name = name
		if email:
			owner_email = email
		break
	itunes_guid = get_first_descendant_text(channel, "podcastGuid")
	last_build_at = parse_rfc2822_datetime(get_first_child_text(channel, "lastBuildDate"))

	return {
		"rss_feed_url": rss_feed_url,
		"title": title,
		"description": description,
		"website_url": website_url,
		"image_url": image_url,
		"language": language,
		# categories -> text[]; keeping None to avoid array formatting issues
		"categories": None,
		"author": author,
		"explicit": explicit,
		"itunes_owner_name": owner_name,
		"itunes_owner_email": owner_email,
		"itunes_podcast_guid": itunes_guid,
		"source": source,
		"last_build_at": last_build_at,
	}


def parse_episode_from_item(item: ET.Element, podcast_id: str, source: str) -> Optional[Dict[str, Any]]:
	guid = get_first_child_text(item, "guid")
	if not guid:
		# Skip episodes without GUID to respect unique (podcast_id, guid)
		return None
	title = get_first_child_text(item, "title") or ""
	description = get_first_child_text(item, "description")
	content_html = get_first_descendant_text(item, "encoded")
	pub_date = parse_rfc2822_datetime(get_first_child_text(item, "pubDate"))
	duration_seconds = parse_duration_to_seconds(get_first_descendant_text(item, "duration"))
	episode_number = parse_int(get_first_descendant_text(item, "episode"))
	season_number = parse_int(get_first_descendant_text(item, "season"))
	episode_type = get_first_descendant_text(item, "episodeType")
	explicit = parse_bool(get_first_descendant_text(item, "explicit"))
	link_url = get_first_child_text(item, "link")
	# enclosure attrs
	audio_url = None
	audio_type = None
	audio_length_bytes: Optional[int] = None
	for child in list(item):
		if child.tag.endswith("enclosure"):
			audio_url = child.attrib.get("url") or audio_url
			audio_type = child.attrib.get("type") or audio_type
			length_val = child.attrib.get("length")
			if length_val and length_val.isdigit():
				audio_length_bytes = int(length_val)
			break
	image_url = get_first_descendant_attr(item, "image", "href")
	# keywords -> text[]; keep None to avoid array formatting issues
	keywords = None
	# transcript url if present
	transcript_url = get_first_descendant_attr(item, "transcript", "url")
	# chapters jsonb: store just the URL if present
	chapters_url = get_first_descendant_attr(item, "chapters", "url")
	chapters = {"url": chapters_url} if chapters_url else None

	return {
		"podcast_id": podcast_id,
		"guid": guid,
		"title": title,
		"description": description,
		"content_html": content_html,
		"pub_date": pub_date,
		"duration_seconds": duration_seconds,
		"episode_number": episode_number,
		"season_number": season_number,
		"episode_type": episode_type,
		"explicit": explicit,
		"audio_url": audio_url,
		"audio_type": audio_type,
		"audio_length_bytes": audio_length_bytes,
		"image_url": image_url,
		"link_url": link_url,
		"keywords": keywords,
		"transcript_url": transcript_url,
		"chapters": chapters,
		"source": source,
	}


def upsert_podcast(client: SupabaseRestClient, record: Dict[str, Any]) -> str:
	resp = client.post(
		path="/podcasts",
		payload=record,
		params={"on_conflict": "rss_feed_url"},
		prefer="resolution=merge-duplicates,return=representation",
	)
	if resp.status_code not in (200, 201):
		raise RuntimeError(f"Failed to upsert podcast: HTTP {resp.status_code} - {resp.text}")
	data = resp.json()
	if isinstance(data, list):
		if not data:
			raise RuntimeError("Upsert podcast returned empty list")
		return data[0]["id"]
	if isinstance(data, dict) and "id" in data:
		return data["id"]
	raise RuntimeError(f"Unexpected podcast upsert response: {data}")


def upsert_episodes(client: SupabaseRestClient, records: List[Dict[str, Any]], chunk_size: int = 200) -> Tuple[int, int]:
	"""Upsert episodes in chunks. Returns (inserted_or_updated_count, chunks_sent)."""
	if not records:
		return 0, 0
	count = 0
	chunks = 0
	for i in range(0, len(records), chunk_size):
		batch = records[i:i + chunk_size]
		resp = client.post(
			path="/episodes",
			payload=batch,
			params={"on_conflict": "podcast_id,guid"},
			prefer="resolution=merge-duplicates,return=representation",
		)
		if resp.status_code not in (200, 201):
			raise RuntimeError(f"Failed to upsert episodes: HTTP {resp.status_code} - {resp.text}")
		data = resp.json()
		if isinstance(data, list):
			count += len(data)
		else:
			# Some PostgREST configs return count in header; keep minimal and count batch
			count += len(batch)
		chunks += 1
		# Light pause to be gentle
		time.sleep(0.05)
	return count, chunks


def process_one_feed(client: SupabaseRestClient, xml_file: Path) -> None:
	profile_basename = derive_profile_basename_from_xml(xml_file)
	rss_feed_url, supplier_name = read_rss_feed_url_from_db(client, profile_basename)
	# If rss_feed_url is not found in database, use a placeholder based on profile_basename
	if not rss_feed_url:
		rss_feed_url = f"file://{profile_basename}"
	# Source from supplier_name or fallback to "unknown"
	source = supplier_name or "unknown"

	# Parse XML
	try:
		tree = ET.parse(str(xml_file))
		root = tree.getroot()
	except Exception as exc:
		print(f"WARNING: Failed to parse XML {xml_file.name}: {exc}")
		return

	# channel node
	channel = None
	for child in list(root):
		if child.tag.endswith("channel"):
			channel = child
			break
	if channel is None:
		raise RuntimeError(f"No <channel> found in {xml_file.name}")

	podcast_record = parse_podcast_from_channel(channel, rss_feed_url=rss_feed_url, source=source)
	podcast_id = upsert_podcast(client, podcast_record)
	print(f"Upserted podcast '{podcast_record['title']}' ({podcast_id}) from {xml_file.name}")

	# Episodes
	episode_records: List[Dict[str, Any]] = []
	for item in get_all_descendants(channel, "item"):
		rec = parse_episode_from_item(item, podcast_id=podcast_id, source=source)
		if rec is not None:
			episode_records.append(rec)

	inserted_count, chunks_used = upsert_episodes(client, episode_records)
	print(f"Upserted {inserted_count} episode rows in {chunks_used} request(s) for {xml_file.name}")


def main() -> None:
	project_root = Path(__file__).resolve().parent
	load_env_file_if_present(project_root)

	SUPABASE_URL = require_env("SUPABASE_URL")
	SUPABASE_SERVICE_ROLE_KEY = require_env("SUPABASE_SERVICE_ROLE_KEY")

	client = SupabaseRestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

	rss_dir = project_root / "temp_rss_output"
	if not rss_dir.exists():
		raise FileNotFoundError(f"temp_rss_output directory not found: {rss_dir}")

	xml_files = sorted([p for p in rss_dir.iterdir() if p.is_file() and p.suffix.lower() == ".xml"])
	if not xml_files:
		print("No RSS XML files found in temp_rss_output.")
		return

	for xml_file in xml_files:
		try:
			process_one_feed(client, xml_file)
		except Exception as exc:
			# Fail fast per user rules: raise explicit error
			print(f"ERROR: Failed to process {xml_file.name}: {exc}")


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(f"ERROR: {e}")
		sys.exit(1)

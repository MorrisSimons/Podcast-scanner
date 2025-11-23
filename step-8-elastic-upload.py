#!/usr/bin/env python3

"""Upload transcript text files to Elasticsearch with unique keyword metadata."""

import argparse
import os
import re
from pathlib import Path
from typing import Optional

import requests
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

from elasticsearch import Elasticsearch, helpers

TOKEN_PATTERN = re.compile(r"\b\w+\b")


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

    def get(self, path: str, params: Optional[dict] = None) -> requests.Response:
        url = self.base_url + path
        return self.session.get(url, params=params or {}, headers=self.default_headers)


def connect_cassandra(
    host: str,
    username: str,
    password: str,
    keyspace: str
):
    """Connect to Cassandra cluster and return cluster and session."""
    auth = PlainTextAuthProvider(username, password)
    cluster = Cluster(
        [host],
        auth_provider=auth,
        protocol_version=5,
    )
    session = cluster.connect(keyspace)
    return cluster, session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload text transcripts to Elasticsearch with keyword facets.",
    )
    parser.add_argument(
        "--input-dir",
        default="output_speach_to_text",
        help="Directory containing .txt files to index.",
    )
    parser.add_argument(
        "--use-cassandra",
        action="store_true",
        help="Read transcript files from Cassandra instead of local directory.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of files to process (useful for testing).",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding used when reading transcript files.",
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX") or "podcast-transcripts",
        help="Elasticsearch index name to write to.",
    )
    parser.add_argument(
        "--host",
        default="http://100.116.226.118:9200",
        help="Elasticsearch host URL. Defaults to local instance: http://100.116.226.118:9200 (or set ELASTICSEARCH_ENDPOINT env var to override).",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ELASTICSEARCH_APIKEY"),
        help="Elasticsearch API key (defaults to ELASTICSEARCH_APIKEY).",
    )
    parser.add_argument(
        "--delete-index",
        action="store_true",
        help="Delete the target index before indexing new documents.",
    )
    return parser.parse_args()


def collect_documents(directory: Path, encoding: str) -> list[dict[str, object]]:
    txt_files = sorted(directory.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {directory}")

    documents: list[dict[str, object]] = []
    for file_path in txt_files:
        text = file_path.read_text(encoding=encoding)
        unique_keywords = sorted(_unique_tokens(text))
        if not unique_keywords:
            continue
        documents.append(
            {
                "id": str(file_path.relative_to(directory)),
                "filename": file_path.name,
                "path": str(file_path.resolve()),
                "content": text,
                "unique_keywords": unique_keywords,
            }
        )

    if not documents:
        raise ValueError(f"No indexable content found in files under {directory}")

    return documents


def _unique_tokens(text: str) -> set[str]:
    return {match for match in TOKEN_PATTERN.findall(text.lower())}


def collect_documents_from_cassandra(encoding: str, limit: Optional[int] = None) -> list[dict[str, object]]:
    # Cassandra configuration
    cassandra_host = os.getenv("CASSANDRA_HOST")
    cassandra_username = os.getenv("CASSANDRA_USERNAME")
    cassandra_password = os.getenv("CASSANDRA_PASSWORD")
    cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE")
    
    missing = [
        var for var, value in [
            ("CASSANDRA_HOST", cassandra_host),
            ("CASSANDRA_USERNAME", cassandra_username),
            ("CASSANDRA_PASSWORD", cassandra_password),
            ("CASSANDRA_KEYSPACE", cassandra_keyspace),
        ] if not value
    ]
    if missing:
        raise ValueError(f"Missing required Cassandra environment variables: {', '.join(missing)}")
    
    # Supabase configuration
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    
    supabase_client = SupabaseRestClient(supabase_url, supabase_key)
    
    # Connect to Cassandra
    print(f"Connecting to Cassandra at {cassandra_host}...")
    cluster, session = connect_cassandra(
        cassandra_host,
        cassandra_username,
        cassandra_password,
        cassandra_keyspace
    )
    
    documents: list[dict[str, object]] = []
    
    try:
        # Fetch all filenames first (lightweight query)
        print("Fetching all filenames from transcript_files table...")
        filename_query = SimpleStatement("SELECT filename FROM transcript_files", fetch_size=1000)
        filename_result = session.execute(filename_query)
        
        all_filenames = [row.filename for row in filename_result]
        if not all_filenames:
            raise ValueError("No records found in transcript_files table")
        
        # Limit files if specified (for testing)
        if limit:
            all_filenames = all_filenames[:limit]
            print(f"Limiting to first {limit} files for testing...")
        
        print(f"Found {len(all_filenames)} files. Processing...")
        
        # * Extract all episode IDs first
        episode_ids = []
        filename_to_episode_id = {}
        for filename in all_filenames:
            episode_id = filename.rsplit(".", 1)[0] if filename.endswith(".txt") else filename
            episode_ids.append(episode_id)
            filename_to_episode_id[filename] = episode_id
        
        # * Batch fetch all metadata from Supabase (much faster than individual requests)
        print("Fetching metadata from Supabase in batches...")
        metadata_lookup = {}
        batch_size = 100
        
        for i in tqdm(range(0, len(episode_ids), batch_size), desc="Fetching metadata", unit="batch"):
            batch_ids = episode_ids[i:i + batch_size]
            # * PostgREST format: id=in.(value1,value2,value3)
            ids_param = ",".join(batch_ids)
            
            try:
                resp = supabase_client.get(
                    "/episodes",
                    params={
                        "id": f"in.({ids_param})",
                        "select": "id,title,description,pub_date,duration_seconds,episode_number,season_number,audio_url,link_url,keywords,podcasts(id,title,author,categories,image_url,language,rss_feed_url)"
                    }
                )
                
                if resp.status_code == 200:
                    rows = resp.json()
                    for episode_data in rows:
                        episode_id = episode_data.get("id")
                        if episode_id:
                            metadata_lookup[episode_id] = episode_data
            except Exception as e:
                tqdm.write(f"WARNING: Error fetching metadata batch {i//batch_size + 1}: {e}")
                continue
        
        print(f"Fetched metadata for {len(metadata_lookup)} episodes")
        
        # Prepare query to fetch content
        prepared_query = session.prepare("SELECT filename, content FROM transcript_files WHERE filename = ?")
        
        for filename in tqdm(all_filenames, desc="Processing episodes", unit="episode"):
            episode_id = filename_to_episode_id[filename]
            
            try:
                result = session.execute(prepared_query, (filename,))
                row = result.one()
                
                if not row or not row.content:
                    continue
                
                text = row.content
                unique_keywords = sorted(_unique_tokens(text))
                if not unique_keywords:
                    continue
                
                # Get metadata from lookup (already fetched in batches)
                episode_data = metadata_lookup.get(episode_id)
                if not episode_data:
                    continue
                
                # Create a copy to avoid modifying the original
                episode_data = episode_data.copy()
                podcast_data = episode_data.pop("podcasts", None)
                
                doc = {
                    "id": episode_id,
                    "content": text,
                    "unique_keywords": unique_keywords,
                    "episode_id": episode_data.get("id"),
                    "episode_title": episode_data.get("title"),
                    "episode_description": episode_data.get("description"),
                    "episode_pub_date": episode_data.get("pub_date"),
                    "episode_duration_seconds": episode_data.get("duration_seconds"),
                    "episode_number": episode_data.get("episode_number"),
                    "episode_season_number": episode_data.get("season_number"),
                    "episode_audio_url": episode_data.get("audio_url"),
                    "episode_link_url": episode_data.get("link_url"),
                    "episode_keywords": episode_data.get("keywords") or [],
                }
                
                if podcast_data:
                    doc.update({
                        "podcast_id": podcast_data.get("id"),
                        "podcast_title": podcast_data.get("title"),
                        "podcast_author": podcast_data.get("author"),
                        "podcast_categories": podcast_data.get("categories") or [],
                        "podcast_image_url": podcast_data.get("image_url"),
                        "podcast_language": podcast_data.get("language"),
                        "podcast_rss_feed_url": podcast_data.get("rss_feed_url"),
                    })
                
                documents.append(doc)
            except Exception as e:
                tqdm.write(f"WARNING: Error processing {episode_id}: {e}")
                continue
        
    finally:
        cluster.shutdown()
    
    if not documents:
        raise ValueError("No indexable documents collected from Cassandra")
    
    print(f"Collected {len(documents)} documents with metadata")
    return documents


def connect(host: str, api_key: Optional[str]) -> Elasticsearch:
    import elasticsearch
    
    # Check if client version matches server version
    client_version = elasticsearch.__version__
    if client_version[0] == 9:
        # Version 9 client requires Elasticsearch 9 server
        # For Elasticsearch 8, we need elasticsearch-py 8.x
        raise ValueError(
            f"Version mismatch: elasticsearch-py {client_version[0]}.x is installed, "
            f"but Elasticsearch 8.x requires elasticsearch-py 8.x. "
            f"Please install compatible version: pip install 'elasticsearch>=8.0.0,<9.0.0'"
        )
    
    # Determine if this is an HTTP (local) or HTTPS (cloud) connection
    is_local = host.startswith("http://")
    
    # Base configuration for all connections
    es_config = {
        "hosts": [host],
    }
    
    if is_local:
        # Local Elasticsearch instance without security
        # SSL is automatically disabled for HTTP URLs
        # Don't verify certificates for local instances
        es_config.update({
            "verify_certs": False,
            "ssl_show_warn": False,
        })
    else:
        # Cloud/managed Elasticsearch instance (HTTPS)
        # SSL is automatically enabled for HTTPS URLs
        es_config.update({
            "verify_certs": True,
        })
        if api_key:
            es_config["api_key"] = api_key
    
    client = Elasticsearch(**es_config)
    
    try:
        if not client.ping():
            raise ConnectionError(f"Failed to reach Elasticsearch at {host}. Is Elasticsearch running?")
    except Exception as e:
        error_msg = str(e)
        # Check for version mismatch error
        if "compatible-with=9" in error_msg and "version 8 or 7" in error_msg:
            raise ValueError(
                "Version mismatch: elasticsearch-py 9.x is incompatible with Elasticsearch 8.x. "
                "Please install elasticsearch-py 8.x: pip install 'elasticsearch>=8.0.0,<9.0.0'"
            ) from e
        raise ConnectionError(f"Failed to connect to Elasticsearch at {host}: {e}") from e
    
    return client


def ensure_index(client: Elasticsearch, index_name: str, delete_existing: bool) -> None:
    # * Validate index name
    if not index_name or not index_name.strip():
        raise ValueError("Elasticsearch index name cannot be empty. Set ELASTICSEARCH_INDEX env var or use --index argument.")
    
    if delete_existing and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if client.indices.exists(index=index_name):
        return

    client.indices.create(
        index=index_name,
        mappings={
            "properties": {
                "filename": {"type": "keyword"},
                "path": {"type": "keyword"},
                "content": {"type": "text"},
                "unique_keywords": {"type": "keyword"},
                "episode_id": {"type": "keyword"},
                "episode_title": {"type": "text"},
                "episode_description": {"type": "text"},
                "episode_pub_date": {"type": "date"},
                "episode_duration_seconds": {"type": "integer"},
                "episode_number": {"type": "integer"},
                "episode_season_number": {"type": "integer"},
                "episode_audio_url": {"type": "keyword"},
                "episode_link_url": {"type": "keyword"},
                "episode_keywords": {"type": "keyword"},
                "podcast_id": {"type": "keyword"},
                "podcast_title": {"type": "text"},
                "podcast_author": {"type": "text"},
                "podcast_categories": {"type": "keyword"},
                "podcast_image_url": {"type": "keyword"},
                "podcast_language": {"type": "keyword"},
                "podcast_rss_feed_url": {"type": "keyword"},
            }
        },
    )


def bulk_index(
    client: Elasticsearch, index_name: str, documents: list[dict[str, object]]
) -> None:
    actions = (
        {
            "_index": index_name,
            "_id": doc["id"],
            "_source": {k: v for k, v in doc.items() if k != "id"},
        }
        for doc in documents
    )
    helpers.bulk(client, actions)


def main() -> None:
    args = parse_args()

    if args.use_cassandra:
        documents = collect_documents_from_cassandra(args.encoding, args.limit)
    else:
        input_dir = Path(args.input_dir).expanduser().resolve()
        if not input_dir.exists() or not input_dir.is_dir():
            raise NotADirectoryError(f"Input directory does not exist: {input_dir}")
        documents = collect_documents(input_dir, args.encoding)

    client = connect(args.host, args.api_key)
    ensure_index(client, args.index, args.delete_index)
    bulk_index(client, args.index, documents)
    print(f"Indexed {len(documents)} documents into '{args.index}' at {args.host}")


if __name__ == "__main__":
    main()

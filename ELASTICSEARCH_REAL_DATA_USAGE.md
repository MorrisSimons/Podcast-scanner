# Elasticsearch Real Data Upload

## Overview

The `step-8-elastic-upload.py` script now supports uploading real transcript data from Scaleway S3 with full metadata enrichment from Supabase.

## How It Works

### Primary Key Mapping

Each `.txt` file in Scaleway S3 uses the episode UUID as its filename:
- S3 filename: `abc123-def456-789.txt`
- Episode ID (primary key): `abc123-def456-789`
- The script strips `.txt` to get the episode UUID

### Data Flow

1. **List S3 Files**: Lists all `.txt` files from Scaleway S3 bucket
2. **Extract Episode ID**: Removes `.txt` extension from filename to get episode UUID
3. **Fetch Metadata**: Queries Supabase using episode UUID as primary key
4. **Join Podcast Data**: Uses embedded resource expansion to fetch related podcast data via foreign key `episodes.podcast_id → podcasts.id`
5. **Enrich Document**: Combines transcript text with episode and podcast metadata
6. **Index to Elasticsearch**: Uploads enriched documents with full metadata

### Supabase Query

The script uses a single REST query with embedded expansion:
```
GET /episodes?id=eq.<uuid>&select=*,podcasts(*)
```

This returns:
- All episode columns from the `episodes` table
- Nested podcast object via the foreign key relationship

### Indexed Fields

**Episode Metadata** (from `episodes` table):
- `episode_id` (keyword)
- `episode_title` (text)
- `episode_description` (text)
- `episode_pub_date` (date)
- `episode_duration_seconds` (integer)
- `episode_number` (integer)
- `episode_season_number` (integer)
- `episode_audio_url` (keyword)
- `episode_link_url` (keyword)
- `episode_keywords` (keyword array)

**Podcast Metadata** (from `podcasts` table via foreign key):
- `podcast_id` (keyword)
- `podcast_title` (text)
- `podcast_author` (text)
- `podcast_categories` (keyword array)
- `podcast_image_url` (keyword)
- `podcast_language` (keyword)
- `podcast_rss_feed_url` (keyword)

**Transcript Data**:
- `content` (text) - full transcript
- `unique_keywords` (keyword array) - unique words extracted from transcript

## Usage

### Test with Sample Data (Local)

```bash
python step-8-elastic-upload.py
```

### Upload Real Data from S3

```bash
python step-8-elastic-upload.py --use-s3 --delete-index
```

### Required Environment Variables

For S3 access:
- `S3_ENDPOINT_URL`
- `S3_BUCKET`
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_REGION`
- `S3_PREFIX` (optional)

For Supabase access:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

For Elasticsearch:
- `ELASTICSEARCH_ENDPOINT`
- `ELASTICSEARCH_APIKEY`
- `ELASTICSEARCH_INDEX` (optional, defaults to "podcast-transcripts")

## Example Output

```
Listing .txt files from S3...
Found 1523 .txt files in S3
[1/1523] Processing episode abc123-def456-789...
[2/1523] Processing episode def456-789-012...
...
Collected 1523 documents with metadata
Indexed 1523 documents into 'podcast-transcripts' at https://...
```


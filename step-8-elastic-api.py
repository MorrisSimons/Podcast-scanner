#!/usr/bin/env python3

"""
FastAPI backend API for searching podcast transcripts in Elasticsearch.
Run with: uvicorn step-8-elastic-api:app --host 0.0.0.0 --port 8000

Dependencies (install with: pip install fastapi uvicorn slowapi python-dotenv):
- fastapi: Web framework
- uvicorn: ASGI server
- slowapi: Rate limiting
- python-dotenv: Environment variable loading
- elasticsearch: Elasticsearch client

Security features:
- Rate limiting (configurable per endpoint)
- Optional API key authentication (set API_KEY env var to enable)
- Configurable CORS origins (set CORS_ORIGINS env var, comma-separated)

Environment variables:
- API_KEY: Optional API key for authentication (leave unset to disable)
- CORS_ORIGINS: Comma-separated list of allowed origins (default: "*")
- ELASTICSEARCH_ENDPOINT: Elasticsearch server URL (default: http://100.116.226.118:9200)
- ELASTICSEARCH_APIKEY: Elasticsearch API key (if needed)

Server access:
- Send requests to: http://YOUR_SERVER:8000/api/search?keyword=SEARCH_TERM
- If API key is enabled, include header: X-API-Key: YOUR_API_KEY
  or: Authorization: Bearer YOUR_API_KEY
- Rate limits: 100 requests/minute per IP (or per API key if enabled)
"""

import os
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader, HTTPBearer
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

load_dotenv()

from elasticsearch import Elasticsearch

app = FastAPI(title="Podcast Transcript Search API", version="1.0.0")

# * API key authentication (optional, enabled if API_KEY env var is set)
API_KEY = os.getenv("API_KEY")  # Set this to enable API key authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


def get_rate_limit_key(request: Request) -> str:
    """Get identifier for rate limiting (API key if provided, else IP address)."""
    # Check for X-API-Key header
    api_key = request.headers.get("X-API-Key")
    if api_key and API_KEY and api_key == API_KEY:
        return f"api_key:{api_key[:8]}"
    
    # Check for Bearer token
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        bearer_token = auth_header[7:]
        if API_KEY and bearer_token == API_KEY:
            return f"api_key:{bearer_token[:8]}"
    
    # Fallback to IP address
    return get_remote_address(request)


# * Rate limiting setup (uses API key if provided, otherwise IP)
limiter = Limiter(key_func=get_rate_limit_key)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


async def verify_api_key(
    api_key: Optional[str] = Security(api_key_header),
    bearer: Optional[HTTPBearer] = Security(bearer_scheme),
) -> Optional[str]:
    """Verify API key from header or bearer token."""
    if not API_KEY:
        # API key authentication is disabled
        return None
    
    # Check X-API-Key header
    if api_key and api_key == API_KEY:
        return api_key
    
    # Check Bearer token (HTTPBearer returns HTTPAuthorizationCredentials object)
    if bearer is not None:
        bearer_token = bearer.credentials
        if bearer_token == API_KEY:
            return bearer_token
    
    # If API key is required but not provided or invalid
    raise HTTPException(
        status_code=401,
        detail="Invalid or missing API key. Include X-API-Key header or Authorization: Bearer TOKEN"
    )


# * CORS configuration
allowed_origins_env = os.getenv("CORS_ORIGINS", "*")
allowed_origins = allowed_origins_env.split(",") if allowed_origins_env != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# Response models
class EpisodeResult(BaseModel):
    episode_id: str
    episode_title: Optional[str] = None
    episode_description: Optional[str] = None
    episode_pub_date: Optional[str] = None
    episode_duration_seconds: Optional[int] = None
    podcast_title: Optional[str] = None
    podcast_author: Optional[str] = None
    podcast_image_url: Optional[str] = None
    score: float
    content_snippet: Optional[str] = None


class SearchResponse(BaseModel):
    keyword: str
    total: int
    results: list[EpisodeResult]


def connect_elasticsearch() -> Elasticsearch:
    """Connect to Elasticsearch instance."""
    import elasticsearch
    
    # Check version compatibility
    client_version = elasticsearch.__version__
    if client_version[0] == 9:
        raise ValueError(
            f"Version mismatch: elasticsearch-py {client_version[0]}.x is installed, "
            f"but Elasticsearch 8.x requires elasticsearch-py 8.x. "
            f"Please install: pip install 'elasticsearch>=8.0.0,<9.0.0'"
        )
    
    host = os.getenv("ELASTICSEARCH_ENDPOINT", "http://100.116.226.118:9200")
    # * Validate host URL - if empty or None, use default
    if not host or not host.strip():
        host = "http://100.116.226.118:9200"
    
    # * Validate URL format
    if not (host.startswith("http://") or host.startswith("https://")):
        raise ValueError(
            f"Invalid ELASTICSEARCH_ENDPOINT format: '{host}'. "
            f"Must start with http:// or https://"
        )
    
    is_local = host.startswith("http://")
    
    es_config = {"hosts": [host]}
    
    if is_local:
        es_config.update({
            "verify_certs": False,
            "ssl_show_warn": False,
        })
    else:
        es_config.update({"verify_certs": True})
        api_key = os.getenv("ELASTICSEARCH_APIKEY")
        if api_key:
            es_config["api_key"] = api_key
    
    client = Elasticsearch(**es_config)
    
    try:
        if not client.ping():
            raise ConnectionError(f"Failed to reach Elasticsearch at {host}")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Elasticsearch at {host}: {e}") from e
    
    return client


# * Lazy Elasticsearch client dependency (connects on first use, not at startup)
_es_client: Optional[Elasticsearch] = None


def get_elasticsearch_client() -> Elasticsearch:
    """Get Elasticsearch client, creating connection on first use."""
    global _es_client
    if _es_client is None:
        _es_client = connect_elasticsearch()
    return _es_client


@app.get("/", response_model=dict)
def root():
    """API root endpoint with security information."""
    api_key_enabled = bool(API_KEY)
    return {
        "name": "Podcast Transcript Search API",
        "version": "1.0.0",
        "security": {
            "api_key_required": api_key_enabled,
            "rate_limiting": {
                "search": "100 requests/minute",
                "episode": "100 requests/minute",
                "health": "30 requests/minute"
            }
        },
        "endpoints": {
            "search": "/api/search?keyword=YOUR_KEYWORD&limit=10",
            "episode": "/api/episode/{episode_id}",
            "health": "/health"
        },
        "authentication": {
            "header": "X-API-Key: YOUR_API_KEY",
            "bearer": "Authorization: Bearer YOUR_API_KEY"
        } if api_key_enabled else {"note": "API key authentication is disabled"}
    }


@app.get("/health", response_model=dict)
@limiter.limit("30/minute")  # Health check has higher rate limit
def health_check(request: Request):
    """Health check endpoint. No authentication required."""
    try:
        es_client = get_elasticsearch_client()
        if es_client.ping():
            return {"status": "healthy", "elasticsearch": "connected"}
        else:
            return {"status": "unhealthy", "elasticsearch": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/api/search", response_model=SearchResponse)
@limiter.limit("100/minute")  # * 100 requests per minute per IP/API key
def search_transcripts(
    request: Request,
    keyword: str = Query(..., description="Keyword to search for in transcripts"),
    index: str = Query(
        default="podcast-transcripts",
        description="Elasticsearch index name"
    ),
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of results to return (1-100)"
    ),
    api_key: Optional[str] = Depends(verify_api_key),  # Optional authentication
):
    """
    Search for a keyword in podcast transcripts.
    
    Returns episodes that contain the keyword in their transcripts.
    """
    try:
        es_client = get_elasticsearch_client()
        # Search Elasticsearch
        response = es_client.search(
            index=index,
            query={"term": {"unique_keywords": keyword.lower()}},
            size=limit,
        )
        
        hits = response.get("hits", {}).get("hits", [])
        
        # Format results
        results = []
        for hit in hits:
            source = hit.get("_source", {})
            
            # Get content snippet (first 300 chars)
            content = source.get("content", "")
            content_snippet = content[:300].replace("\n", " ")
            if len(content) > 300:
                content_snippet += "..."
            
            results.append(EpisodeResult(
                episode_id=source.get("episode_id", hit.get("_id", "")),
                episode_title=source.get("episode_title"),
                episode_description=source.get("episode_description"),
                episode_pub_date=source.get("episode_pub_date"),
                episode_duration_seconds=source.get("episode_duration_seconds"),
                podcast_title=source.get("podcast_title"),
                podcast_author=source.get("podcast_author"),
                podcast_image_url=source.get("podcast_image_url"),
                score=hit.get("_score", 0.0),
                content_snippet=content_snippet if content_snippet else None,
            ))
        
        return SearchResponse(
            keyword=keyword,
            total=len(results),
            results=results
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/api/episode/{episode_id}", response_model=dict)
@limiter.limit("100/minute")  # * 100 requests per minute per IP/API key
def get_episode(
    request: Request,
    episode_id: str,
    index: str = Query(default="podcast-transcripts"),
    api_key: Optional[str] = Depends(verify_api_key),  # Optional authentication
):
    """
    Get full details of a specific episode by ID.
    """
    try:
        es_client = get_elasticsearch_client()
        response = es_client.get(index=index, id=episode_id)
        return response["_source"]
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Episode not found: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "step-8-elastic-api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


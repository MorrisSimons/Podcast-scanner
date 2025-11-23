#!/usr/bin/env python3
"""
Test script for the Podcast Transcript Search API.
Can test both localhost and external URLs.
Usage:
    python3 test-api-external.py                    # Test localhost
    python3 test-api-external.py --url http://your-server.com:8000
    python3 test-api-external.py --url http://your-server.com:8000 --api-key YOUR_KEY
"""

import argparse
import json
import sys
import time
from typing import Optional

try:
    import requests
except ImportError:
    print("Error: requests library not installed. Install with: pip install requests")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")


def print_success(text: str):
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")


def print_error(text: str):
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")


def print_warning(text: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")


def print_info(text: str):
    """Print info message."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.RESET}")


class APITester:
    """Test suite for the Podcast Transcript Search API."""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: int = 10):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.results = []
        
    def get_headers(self) -> dict:
        """Get request headers with optional API key."""
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
    
    def test_connectivity(self) -> bool:
        """Test basic connectivity to the API server."""
        print_header("Test 1: Basic Connectivity")
        try:
            response = requests.get(
                f"{self.base_url}/",
                timeout=self.timeout,
                headers=self.get_headers()
            )
            if response.status_code == 200:
                print_success(f"Server is reachable at {self.base_url}")
                print_info(f"Response time: {response.elapsed.total_seconds():.3f}s")
                self.results.append(("Connectivity", True))
                return True
            else:
                print_error(f"Server returned status {response.status_code}")
                self.results.append(("Connectivity", False))
                return False
        except requests.exceptions.Timeout:
            print_error(f"Connection timeout after {self.timeout}s")
            self.results.append(("Connectivity", False))
            return False
        except requests.exceptions.ConnectionError as e:
            print_error(f"Connection failed: {e}")
            self.results.append(("Connectivity", False))
            return False
        except Exception as e:
            print_error(f"Unexpected error: {e}")
            self.results.append(("Connectivity", False))
            return False
    
    def test_root_endpoint(self) -> bool:
        """Test the root endpoint."""
        print_header("Test 2: Root Endpoint (/)")
        try:
            response = requests.get(
                f"{self.base_url}/",
                timeout=self.timeout,
                headers=self.get_headers()
            )
            
            print_info(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print_success("Root endpoint responded successfully")
                print_info(f"API Name: {data.get('name', 'N/A')}")
                print_info(f"Version: {data.get('version', 'N/A')}")
                
                # Check security info
                security = data.get('security', {})
                api_key_required = security.get('api_key_required', False)
                if api_key_required:
                    print_warning("API key authentication is enabled")
                else:
                    print_info("API key authentication is disabled")
                
                # Show endpoints
                endpoints = data.get('endpoints', {})
                print_info("Available endpoints:")
                for name, path in endpoints.items():
                    print(f"  - {name}: {path}")
                
                self.results.append(("Root Endpoint", True))
                return True
            else:
                print_error(f"Unexpected status code: {response.status_code}")
                print_error(f"Response: {response.text[:200]}")
                self.results.append(("Root Endpoint", False))
                return False
                
        except Exception as e:
            print_error(f"Error testing root endpoint: {e}")
            self.results.append(("Root Endpoint", False))
            return False
    
    def test_health_endpoint(self) -> bool:
        """Test the health check endpoint."""
        print_header("Test 3: Health Check Endpoint (/health)")
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=self.timeout,
                headers=self.get_headers()
            )
            
            print_info(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'unknown')
                es_status = data.get('elasticsearch', 'unknown')
                
                if status == 'healthy':
                    print_success(f"API Status: {status}")
                else:
                    print_warning(f"API Status: {status}")
                
                if es_status == 'connected':
                    print_success(f"Elasticsearch: {es_status}")
                else:
                    print_warning(f"Elasticsearch: {es_status}")
                    if 'error' in data:
                        print_error(f"Elasticsearch Error: {data['error']}")
                
                self.results.append(("Health Check", True))
                return True
            else:
                print_error(f"Unexpected status code: {response.status_code}")
                self.results.append(("Health Check", False))
                return False
                
        except Exception as e:
            print_error(f"Error testing health endpoint: {e}")
            self.results.append(("Health Check", False))
            return False
    
    def test_search_endpoint(self, keyword: str = "kvartal", limit: int = 5) -> bool:
        """Test the search endpoint."""
        print_header(f"Test 4: Search Endpoint (/api/search)")
        try:
            params = {
                "keyword": keyword,
                "limit": limit
            }
            
            response = requests.get(
                f"{self.base_url}/api/search",
                params=params,
                timeout=self.timeout,
                headers=self.get_headers()
            )
            
            print_info(f"Status Code: {response.status_code}")
            print_info(f"Search Keyword: {keyword}")
            print_info(f"Limit: {limit}")
            
            if response.status_code == 200:
                data = response.json()
                total = data.get('total', 0)
                results = data.get('results', [])
                
                print_success(f"Search completed successfully")
                print_info(f"Total Results: {total}")
                print_info(f"Returned Results: {len(results)}")
                
                if results:
                    print_info("\nFirst 3 results:")
                    for i, result in enumerate(results[:3], 1):
                        print(f"  [{i}] {result.get('episode_title', 'N/A')}")
                        print(f"      Podcast: {result.get('podcast_title', 'N/A')}")
                        print(f"      Score: {result.get('score', 0)}")
                else:
                    print_warning("No results returned")
                
                self.results.append(("Search Endpoint", True))
                return True
            elif response.status_code == 401:
                print_error("Authentication required (401 Unauthorized)")
                print_warning("Try adding --api-key YOUR_KEY if API key is enabled")
                self.results.append(("Search Endpoint", False))
                return False
            else:
                print_error(f"Unexpected status code: {response.status_code}")
                print_error(f"Response: {response.text[:200]}")
                self.results.append(("Search Endpoint", False))
                return False
                
        except Exception as e:
            print_error(f"Error testing search endpoint: {e}")
            self.results.append(("Search Endpoint", False))
            return False
    
    def test_episode_endpoint(self, episode_id: str) -> bool:
        """Test the episode endpoint."""
        print_header(f"Test 5: Episode Endpoint (/api/episode/{episode_id})")
        try:
            response = requests.get(
                f"{self.base_url}/api/episode/{episode_id}",
                timeout=self.timeout,
                headers=self.get_headers()
            )
            
            print_info(f"Status Code: {response.status_code}")
            print_info(f"Episode ID: {episode_id}")
            
            if response.status_code == 200:
                data = response.json()
                print_success("Episode retrieved successfully")
                print_info(f"Title: {data.get('episode_title', 'N/A')}")
                print_info(f"Podcast: {data.get('podcast_title', 'N/A')}")
                print_info(f"Published: {data.get('episode_pub_date', 'N/A')}")
                
                self.results.append(("Episode Endpoint", True))
                return True
            elif response.status_code == 404:
                print_warning("Episode not found (404)")
                print_info("This might be expected if the episode_id doesn't exist")
                self.results.append(("Episode Endpoint", False))
                return False
            elif response.status_code == 401:
                print_error("Authentication required (401 Unauthorized)")
                self.results.append(("Episode Endpoint", False))
                return False
            else:
                print_error(f"Unexpected status code: {response.status_code}")
                self.results.append(("Episode Endpoint", False))
                return False
                
        except Exception as e:
            print_error(f"Error testing episode endpoint: {e}")
            self.results.append(("Episode Endpoint", False))
            return False
    
    def test_rate_limiting(self) -> bool:
        """Test rate limiting by making rapid requests."""
        print_header("Test 6: Rate Limiting")
        try:
            print_info("Making 5 rapid requests to test rate limiting...")
            success_count = 0
            rate_limited = False
            
            for i in range(5):
                response = requests.get(
                    f"{self.base_url}/health",
                    timeout=self.timeout,
                    headers=self.get_headers()
                )
                if response.status_code == 200:
                    success_count += 1
                elif response.status_code == 429:
                    rate_limited = True
                    print_warning(f"Request {i+1}: Rate limited (429)")
                    break
                time.sleep(0.1)
            
            if rate_limited:
                print_success("Rate limiting is working correctly")
                self.results.append(("Rate Limiting", True))
                return True
            else:
                print_info(f"All {success_count} requests succeeded (rate limit not hit)")
                print_warning("Rate limiting may not be active or limit is high")
                self.results.append(("Rate Limiting", True))
                return True
                
        except Exception as e:
            print_error(f"Error testing rate limiting: {e}")
            self.results.append(("Rate Limiting", False))
            return False
    
    def run_all_tests(self, test_episode_id: Optional[str] = None) -> dict:
        """Run all tests and return summary."""
        print_header("Starting API Test Suite")
        print_info(f"Testing API at: {self.base_url}")
        if self.api_key:
            print_info(f"Using API key: {self.api_key[:8]}...")
        else:
            print_info("No API key provided")
        
        # Run tests
        self.test_connectivity()
        time.sleep(0.5)
        
        self.test_root_endpoint()
        time.sleep(0.5)
        
        self.test_health_endpoint()
        time.sleep(0.5)
        
        self.test_search_endpoint()
        time.sleep(0.5)
        
        if test_episode_id:
            self.test_episode_endpoint(test_episode_id)
        else:
            print_header("Test 5: Episode Endpoint (Skipped)")
            print_warning("No episode_id provided, skipping episode endpoint test")
            print_info("Use --episode-id to test this endpoint")
            self.results.append(("Episode Endpoint", None))
        
        time.sleep(0.5)
        
        self.test_rate_limiting()
        
        # Print summary
        print_header("Test Summary")
        passed = sum(1 for _, result in self.results if result is True)
        failed = sum(1 for _, result in self.results if result is False)
        skipped = sum(1 for _, result in self.results if result is None)
        
        for test_name, result in self.results:
            if result is True:
                print_success(f"{test_name}: PASSED")
            elif result is False:
                print_error(f"{test_name}: FAILED")
            else:
                print_warning(f"{test_name}: SKIPPED")
        
        print(f"\n{Colors.BOLD}Total: {passed} passed, {failed} failed, {skipped} skipped{Colors.RESET}")
        
        return {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "total": len(self.results)
        }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test the Podcast Transcript Search API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test localhost
  python3 test-api-external.py
  
  # Test external server
  python3 test-api-external.py --url http://your-server.com:8000
  
  # Test with API key
  python3 test-api-external.py --url http://your-server.com:8000 --api-key YOUR_KEY
  
  # Test with specific episode ID
  python3 test-api-external.py --episode-id 9357384f-421a-429a-9453-a282860dbeed
        """
    )
    
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Base URL of the API (default: http://localhost:8000)"
    )
    
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key for authentication (if required)"
    )
    
    parser.add_argument(
        "--episode-id",
        default=None,
        help="Episode ID to test the episode endpoint"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Request timeout in seconds (default: 10)"
    )
    
    args = parser.parse_args()
    
    tester = APITester(
        base_url=args.url,
        api_key=args.api_key,
        timeout=args.timeout
    )
    
    summary = tester.run_all_tests(test_episode_id=args.episode_id)
    
    # Exit with appropriate code
    sys.exit(0 if summary["failed"] == 0 else 1)


if __name__ == "__main__":
    main()



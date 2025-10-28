import requests
import json
import os
from pathlib import Path


# Process each JSON file in the output folder
for json_file in Path("output").glob("*.json"):
    with open(json_file, "r") as f:
        data = json.load(f)
    
    if "rssFeedUrl" not in data:
        print(f"No rssFeedUrl found in {json_file.name}")
        continue
    
    rss_url = data["rssFeedUrl"]
    podcast_name = data.get("podcastName", json_file.stem)
    
    print(f"Fetching RSS feed for: {podcast_name}")
    
    payload = {}
    headers = {}
    
    response = requests.request("GET", rss_url, headers=headers, data=payload)
    
    # Save the RSS feed response
    output_file = rf"{json_file.stem}_rss.xml"
    with open(f"output_rss/{output_file}", "w") as f:
        f.write(response.text)
    
    print(f"Saved to {output_file}")

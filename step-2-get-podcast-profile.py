import requests
import json
import os
# 
url = "https://api.mediafacts.se/api/podcast/v1/podcasts/details?id=857538db-0c16-4f7d-b053-08dc85405cb3&fromweek=43&fromyear=2025"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

# Extract only the fields we need
fields = ["rssFeedUrl", "podcastName", "supplierId", "supplierName", "networkId", "networkName", "genre"]
data = response.json()
filtered_data = {field: data.get(field) for field in fields}

os.makedirs("output", exist_ok=True)
with open("output/podcast_profile.json", "w") as f:
    json.dump(filtered_data, f, indent=2, ensure_ascii=False)

print("Response saved to output/podcast_profile.json")

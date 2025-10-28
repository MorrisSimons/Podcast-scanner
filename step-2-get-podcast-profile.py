import requests
import json
import os
# 
id = "3e5a848e-29c0-4520-4db3-08dc8570d9ab"
id_podplay = "2677c77a-3e1f-42bc-05bc-08dc856248f9"
url = f"https://api.mediafacts.se/api/podcast/v1/podcasts/details?id={id_podplay}&fromweek=43&fromyear=2025"

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

import requests
import json

url = "https://api.mediafacts.se/api/podcast/v1/podcasts"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

with open("podcasts.json", "w") as f:
    json.dump(response.json(), f, indent=2, ensure_ascii=False)

print("Response saved to podcasts.json")

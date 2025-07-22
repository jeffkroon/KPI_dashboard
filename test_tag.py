import requests
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.gripp.com/public/api3.php"
GRIPP_API_KEY = os.getenv("GRIPP_API_KEY")
HEADERS = {"Authorization": f"Bearer {GRIPP_API_KEY}"}

def fetch_companies_with_tags_check():
    payload = [{
        "id": 1,
        "method": "company.get",
        "params": [
            [],
            {"paging": {"firstresult": 0, "maxresults": 10},
             "fields": ["id", "companyname", "tags"]}
        ]
    }]
    
    response = requests.post(BASE_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()[0]["result"]["rows"]
    
    print("\n[DEBUG] Tags check per bedrijf:")
    for company in data:
        print(f"Company ID: {company.get('id')}, Tags: {company.get('tags')}")

if __name__ == "__main__":
    fetch_companies_with_tags_check()
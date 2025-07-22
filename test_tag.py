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


# Nieuwe functie om het aantal bedrijven per primaire tag te tellen
def count_companies_by_primary_tag():
    eigen_tag = "1 | Eigen webshop(s) / bedrijven"
    klanten_tag = "1 | Externe opdrachten / contracten"

    eigen_count = 0
    klanten_count = 0
    total_companies = 0
    firstresult = 0
    batch_size = 250

    while True:
        payload = [{
            "id": 1,
            "method": "company.get",
            "params": [
                [],
                {"paging": {"firstresult": firstresult, "maxresults": batch_size},
                 "fields": ["id", "companyname", "tags"]}
            ]
        }]
        
        response = requests.post(BASE_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        response_data = response.json()

        if "result" not in response_data[0] or "rows" not in response_data[0]["result"]:
            print("\n❌ API Error of geen data:", response_data)
            break

        rows = response_data[0]["result"]["rows"]
        if not rows:
            break

        total_companies += len(rows)

        for company in rows:
            tags = company.get("tags")
            if isinstance(tags, list):
                tag_names = [tag.get("searchname", "").strip() for tag in tags]
                if eigen_tag in tag_names:
                    eigen_count += 1
                elif klanten_tag in tag_names:
                    klanten_count += 1
                elif "Bedrijf | Algemeen mailadres" in tag_names:
                    print(f"🟡 Alleen Algemeen mailadres - ID {company.get('id')}, Naam: {company.get('companyname')}, Tags: {tag_names}")

        if len(rows) < batch_size:
            break  # laatste pagina bereikt
        firstresult += batch_size

    print(f"\n✅ [DEBUG] Totaal bedrijven opgehaald: {total_companies}")
    print(f"[DEBUG] Aantal Eigen bedrijven: {eigen_count}")
    print(f"[DEBUG] Aantal Klanten: {klanten_count}")

if __name__ == "__main__":
    fetch_companies_with_tags_check()
    count_companies_by_primary_tag()
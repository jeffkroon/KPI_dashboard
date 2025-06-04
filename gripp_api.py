import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Callable
from sqlalchemy import create_engine, text
from supabase import create_client, Client
load_dotenv()
MOCK_MODE = False  # Zet op False voor live API-verzoeken

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

INVOICE_CACHE_PATH = "data/gripp_invoices.parquet"

def upload_uren_to_supabase(data: list[dict]):
    res = supabase.table("urenregistratie").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload mislukt:", res.json())
    else:
        print(f"✅ {len(data)} urenrecords geüpload naar Supabase")

# Nieuwe functies voor uploaden naar Supabase
def upload_projects_to_supabase(data: list[dict]):
    res = supabase.table("projects").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload projecten mislukt:", res.json())
    else:
        print(f"✅ {len(data)} projectrecords geüpload naar Supabase")

def upload_employees_to_supabase(data: list[dict]):
    res = supabase.table("employees").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload medewerkers mislukt:", res.json())
    else:
        print(f"✅ {len(data)} medewerkers geüpload naar Supabase")

def upload_companies_to_supabase(data: list[dict]):
    res = supabase.table("companies").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload bedrijven mislukt:", res.json())
    else:
        print(f"✅ {len(data)} bedrijven geüpload naar Supabase")

def upload_invoices_to_supabase(data: list[dict]):
    res = supabase.table("invoices").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload facturen mislukt:", res.json())
    else:
        print(f"✅ {len(data)} facturen geüpload naar Supabase")

POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

BASE_URL = "https://api.gripp.com/public/api3.php"
API_KEY = os.getenv("GRIPP_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
CACHE_PATH = "data/gripp_hours.parquet"
MAX_CACHE_AGE_MINUTES = 30
CACHE_DIR = "data"
os.makedirs(CACHE_DIR, exist_ok=True)

def fetch_gripp_invoices(last_sync_date=None):
    if MOCK_MODE:
        print("📦 MOCK: invoices geladen uit dummy bestand.")
        return pd.read_csv("mock_data/invoices.csv")
    # Stap 1: Bedrijf ophalen
    payload_company = [{
        "id": 1,
        "method": "company.get",
        "params": [
            [],
            {
                "paging": {
                    "firstresult": 0,
                    "maxresults": 1
                }
            }
        ]
    }]
    response_company = requests.post(BASE_URL, headers=HEADERS, json=payload_company)
    remaining = response_company.headers.get("X-RateLimit-Remaining")
    reset_timestamp = response_company.headers.get("X-RateLimit-Reset")
    if reset_timestamp:
        reset_time = datetime.fromtimestamp(int(reset_timestamp))
        print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
    if remaining is not None:
        print(f"📉 Remaining requests: {remaining}")
    print(f"🕒 Remaining requests this hour: {remaining if remaining else 'Onbekend'}")
    print(f"🔢 Hourly rate limit: {response_company.headers.get('X-RateLimit-Limit', 'Onbekend')}")
    response_company.raise_for_status()
    data_company = response_company.json()
    company_result = data_company[0].get("result", {})
    rows = company_result.get("rows", [])
    if not rows:
        print("❌ Geen klant gevonden.")
        return pd.DataFrame()
    customer_id = rows[0]["id"]

    # Stap 2: Facturen ophalen met volledige paginering
    all_rows = []
    start = 0
    max_results = 100
    watchdog = 50  # failsafe tegen infinite loop

    while watchdog > 0:
        print(f"📦 Ophalen facturen... pagina start: {start}", flush=True)
        payload_invoice = [{
            "id": 2,
            "method": "invoice.get",
            "params": [
                [
                    {"field": "company.id", "operator": "equals", "value": customer_id},
                    {"field": "invoice.totalopeninclvat", "operator": "notequals", "value": "0"}
                ],
                {
                    "paging": {"firstresult": start, "maxresults": max_results},
                    "orderings": [{"field": "invoice.expirydate", "direction": "asc"}]
                }
            ]
        }]
        if last_sync_date:
            payload_invoice[0]["params"][0].append({
                "field": "invoice.modifieddate",
                "operator": "greaterthan",
                "value": last_sync_date.isoformat()
            })
        time.sleep(0.5)
        response = requests.post(BASE_URL, headers=HEADERS, json=payload_invoice)
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_timestamp = response.headers.get("X-RateLimit-Reset")
        if reset_timestamp:
            reset_time = datetime.fromtimestamp(int(reset_timestamp))
            print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
        if remaining is not None:
            print(f"📉 Remaining requests: {remaining}")
        if remaining and remaining.isdigit() and int(remaining) <= 1:
            print("🚨 Bijna aan je limiet.")
            if reset_timestamp:
                wait_seconds = int(reset_timestamp) - int(time.time())
                wait_seconds = max(wait_seconds, 1)
                print(f"⏱️ Wachten tot reset in {wait_seconds} seconden...", flush=True)
                time.sleep(wait_seconds)
            else:
                print("⏱️ Geen reset-tijd bekend, standaard 60 seconden wachten...")
                time.sleep(60)
        response.raise_for_status()
        print("✅ API-call succesvol", flush=True)
        data = response.json()
        rows = data[0].get("result", {}).get("rows", [])
        if not rows:
            break
        all_rows.extend(rows)
        more = data[0]["result"].get("more_items_in_collection", False)
        if not more:
            break
        start = data[0]["result"].get("next_start", start + max_results)
        watchdog -= 1

    if not all_rows:
        print("⚠️ Geen facturen gevonden.")
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    print(f"✅ Totaal opgehaalde facturen: {len(df)}")
    df.to_parquet("data/gripp_invoices.parquet", index=False)
    return df

def is_cache_fresh():
    if not os.path.exists(CACHE_PATH):
        return False
    modified = datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
    return datetime.now() - modified < timedelta(minutes=MAX_CACHE_AGE_MINUTES)

def load_data():
    if is_cache_fresh():
        return pd.read_parquet(CACHE_PATH)
    else:
        df = fetch_gripp_invoices()
        df["snapshot_timestamp"] = datetime.now()
        df.to_sql("gripp_invoices_snapshots", engine, if_exists="append", index=False)
        return df

def cached_fetch(name: str, fetch_fn: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    cache_path = f"data/{name}.parquet"
    if os.path.exists(cache_path):
        modified = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if datetime.now() - modified < timedelta(minutes=MAX_CACHE_AGE_MINUTES):
            return pd.read_parquet(cache_path)
    df = fetch_fn()
    df.to_parquet(cache_path, index=False)
    return df

def fetch_gripp_projects():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: projects geladen uit dummy bestand.")
            return pd.read_csv("mock_data/projects.csv")
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "project.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset_timestamp = response.headers.get("X-RateLimit-Reset")
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
            if remaining is not None:
                print(f"📉 Remaining requests: {remaining}")
            response.raise_for_status()
            data = response.json()
            rows = data[0].get("result", {}).get("rows", [])
            all_rows.extend(rows)
            if not data[0]["result"].get("more_items_in_collection", False):
                break
            start = data[0]["result"].get("next_start", start + max_results)
            watchdog -= 1
        return pd.DataFrame(all_rows)
    return cached_fetch("gripp_projects", fetch)

def fetch_gripp_employees():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: employees geladen uit dummy bestand.")
            return pd.read_csv("mock_data/employees.csv")
        all_rows = []
        start = 0
        max_results = 250
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "employee.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset_timestamp = response.headers.get("X-RateLimit-Reset")
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
            if remaining is not None:
                print(f"📉 Remaining requests: {remaining}")
            response.raise_for_status()
            data = response.json()
            rows = data[0].get("result", {}).get("rows", [])
            all_rows.extend(rows)
            if not data[0]["result"].get("more_items_in_collection", False):
                break
            start = data[0]["result"].get("next_start", start + max_results)
            watchdog -= 1
        return pd.DataFrame(all_rows)
    return cached_fetch("gripp_employees", fetch)

def fetch_gripp_companies():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: companies geladen uit dummy bestand.")
            return pd.read_csv("mock_data/companies.csv")
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "company.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset_timestamp = response.headers.get("X-RateLimit-Reset")
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
            if remaining is not None:
                print(f"📉 Remaining requests: {remaining}")
            response.raise_for_status()
            data = response.json()
            rows = data[0].get("result", {}).get("rows", [])
            all_rows.extend(rows)
            if not data[0]["result"].get("more_items_in_collection", False):
                break
            start = data[0]["result"].get("next_start", start + max_results)
            watchdog -= 1
        return pd.DataFrame(all_rows)
    return cached_fetch("gripp_companies", fetch)

def fetch_gripp_hours_data():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: hours data geladen uit dummy bestand.")
            return pd.read_csv("mock_data/hours.csv")
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "hour.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset_timestamp = response.headers.get("X-RateLimit-Reset")
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                print(f"⏳ Rate limit reset at: {reset_time.strftime('%H:%M:%S')}")
            if remaining is not None:
                print(f"📉 Remaining requests: {remaining}")
            response.raise_for_status()
            data = response.json()
            rows = data[0].get("result", {}).get("rows", [])
            all_rows.extend(rows)
            if not data[0]["result"].get("more_items_in_collection", False):
                break
            start = data[0]["result"].get("next_start", start + max_results)
            watchdog -= 1
        return pd.DataFrame(all_rows)
    return cached_fetch("gripp_hours_data", fetch)


if __name__ == "__main__":
    datasets = {}

    def get_last_sync_date(table_name: str):
        query = f"SELECT MAX(snapshot_timestamp) FROM {table_name}"
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                if result is not None and result[0] is not None:
                    return result[0]
        except Exception as e:
            print(f"⚠️ Kon laatste sync-datum niet ophalen voor {table_name}: {e}")
        return None

    print("⏳ Ophalen van facturen...")
    time.sleep(0.5)
    last_sync_invoices = get_last_sync_date("gripp_invoices")
    def fetch_and_cache_gripp_invoices():
        def fetch():
            df = fetch_gripp_invoices(last_sync_date=last_sync_invoices)
            df.to_parquet(INVOICE_CACHE_PATH, index=False)
            return df
        return cached_fetch("gripp_invoices", fetch)

    datasets["gripp_invoices"] = fetch_and_cache_gripp_invoices()

    print("⏳ Ophalen van projecten...")
    time.sleep(0.5)
    datasets["gripp_projects"] = fetch_gripp_projects()

    print("⏳ Ophalen van medewerkers...")
    time.sleep(0.5)
    datasets["gripp_employees"] = fetch_gripp_employees()

    print("⏳ Ophalen van relaties...")
    time.sleep(0.5)
    datasets["gripp_companies"] = fetch_gripp_companies()

    print("⏳ Ophalen van uren...")
    time.sleep(0.5)
    datasets["gripp_hours_data"] = fetch_gripp_hours_data()

    print(f"\n🔑 API Key geladen: {'gevonden' if API_KEY else 'NIET gevonden'}")

    # Upload urenregistratie naar Supabase
    if not datasets["gripp_hours_data"].empty:
        print(f"⬆️ Uploaden naar Supabase: gripp_hours_data")
        upload_uren_to_supabase(datasets["gripp_hours_data"].to_dict("records"))

    if not datasets["gripp_projects"].empty:
        print(f"⬆️ Uploaden naar Supabase: gripp_projects")
        upload_projects_to_supabase(datasets["gripp_projects"].to_dict("records"))

    if not datasets["gripp_employees"].empty:
        print(f"⬆️ Uploaden naar Supabase: gripp_employees")
        upload_employees_to_supabase(datasets["gripp_employees"].to_dict("records"))

    if not datasets["gripp_companies"].empty:
        print(f"⬆️ Uploaden naar Supabase: gripp_companies")
        upload_companies_to_supabase(datasets["gripp_companies"].to_dict("records"))

    if not datasets["gripp_invoices"].empty:
        print(f"⬆️ Uploaden naar Supabase: gripp_invoices")
        upload_invoices_to_supabase(datasets["gripp_invoices"].to_dict("records"))

    for name, df in datasets.items():
        print(f"\n📊 Dataset: {name}")
        print(df.head(3))
        print(f"Kolommen: {df.columns.tolist()}")
        print(f"Rijen: {len(df)}")
        df["snapshot_timestamp"] = datetime.now()
        df.to_sql(name, engine, if_exists="append", index=False)
        print(f"💾 Dataset {name} opgeslagen in Postgres.")
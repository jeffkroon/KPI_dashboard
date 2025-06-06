import sys
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Callable
from sqlalchemy import create_engine, text
from supabase import create_client, Client
import numpy as np
import json
load_dotenv()
FORCE_REFRESH = "--refresh" in sys.argv
MOCK_MODE = False  # Zet op False voor live API-verzoeken

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# Nieuwe functie om uren en kosten per project te tonen
def print_project_costs_and_hours(project_id: int):
    print(f"\n⏳ Ophalen van uren en kosten voor project ID {project_id}...")
    
    # Ophalen en filteren uren per project
    hours_raw = flatten_dict_column(fetch_gripp_hours_data())
    hours_clean = sanitize_for_supabase(hours_raw)
    hours_filtered = hours_clean[hours_clean["offerprojectbase_id"] == project_id]
    
    # Ophalen en filteren kosten per project
    costs_raw = flatten_dict_column(fetch_gripp_costs())
    costs_clean = sanitize_for_supabase(costs_raw)
    print("Kolommen in costs_clean:", costs_clean.columns.tolist())
    # Gebruik offerprojectbase_id in plaats van project_id als filterkolom
    if "offerprojectbase_id" in costs_clean.columns:
        costs_filtered = costs_clean[costs_clean["offerprojectbase_id"] == project_id]
    else:
        print("⚠️ Geen kolom 'offerprojectbase_id' gevonden in costs dataset.")
        costs_filtered = pd.DataFrame()  # lege dataframe
    
    print(f"\n📊 Uren voor project {project_id} (aantal rijen: {len(hours_filtered)})")
    if not hours_filtered.empty:
        print(f"  Kolomnamen: {hours_filtered.columns.tolist()}")
        import pprint
        pprint.pprint(hours_filtered.head(5).to_dict(orient='records'), indent=2, width=120, compact=False)
    else:
        print("  Geen uren gevonden voor dit project.")
    
    print(f"\n📊 Kosten voor project {project_id} (aantal rijen: {len(costs_filtered)})")
    if not costs_filtered.empty:
        print(f"  Kolomnamen: {costs_filtered.columns.tolist()}")
        import pprint
        pprint.pprint(costs_filtered.head(5).to_dict(orient='records'), indent=2, width=120, compact=False)
    else:
        print("  Geen kosten gevonden voor dit project.")
def filter_projects(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "number", "name", "description", "clientreference",
        "totalinclvat", "totalexclvat", "archived",
        "startdate_date", "deadline_date", "enddate_date",
        "accountmanager_id", "accountmanager_searchname",
        "phase_id", "phase_searchname",
        "company_id", "company_searchname",
        "contact_id", "contact_searchname",
        "updatedon_date", "viewonlineurl"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols]

def filter_employees(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "firstname", "lastname", "searchname", "email", "function", "active",
        "employeesince_date", "department_id", "role_id", "updatedon_date", "identity_id"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols]

def filter_companies(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "companyname", "legalname", "customernumber", "email", "phone", "website",
        "invoiceaddress_street", "invoiceaddress_streetnumber", "invoiceaddress_zipcode", "invoiceaddress_city",
        "invoiceaddress_country", "vatnumber", "cocnumber",
        "accountmanager_id", "accountmanager_searchname",
        "createdon_date", "updatedon_date",
        "visitingaddress_street", "visitingaddress_streetnumber", "visitingaddress_zipcode", "visitingaddress_city"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols]

def filter_tasktypes(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "name", "searchname", "color", "createdon_date", "updatedon_date"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols]

def filter_hours(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "amount", "description", "date_date",
        "employee_id", "employee_searchname",
        "offerprojectbase_id", "offerprojectbase_searchname",
        "task_id", "task_searchname",
        "status_id", "status_searchname",
        "authorizedby_id", "authorizedby_searchname",
        "definitiveon_date", "updatedon_date"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols]
def flatten_dict_column(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = df[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            df = df.drop(columns=[col]).join(expanded)
    return df

# Sanitize dataframe for Supabase upload
def sanitize_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

    # Vervang inf/-inf door np.nan
    df = df.replace([np.inf, -np.inf], np.nan)

    # Recursieve cleaning van geneste dicts/lists met NaN/inf
    def recursive_clean(val):
        if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
            return None
        if isinstance(val, dict):
            return {k: recursive_clean(v) for k, v in val.items()}
        if isinstance(val, list):
            return [recursive_clean(v) for v in val]
        return val

    for col in df.columns:
        df[col] = df[col].apply(recursive_clean)

    # Verwijder kolommen die nog steeds dicts/lists bevatten
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            print(f"⚠️ Kolom met dict/list verwijderd: {col}")
            df.drop(columns=[col], inplace=True)

    # Zet NaN om naar None voor JSON-compliance
    df = df.where(pd.notnull(df), None)
    # Forceer alle waarden die NaN of +/-inf zijn naar None – als fallback
    df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or np.isinf(x)) else x)
    return df
def upload_uren_to_supabase(data: list[dict]):
    try:
        res = supabase.table("urenregistratie").insert(data).execute()
        if res.status_code != 201:
            print("⚠️ Upload mislukt (statuscode):", res.status_code)
            print("⚠️ Response content:", res.json())
        else:
            print(f"✅ {len(data)} urenrecords geüpload naar Supabase")
    except Exception as e:
        print("❌ Fout tijdens upload naar Supabase:")
        print(e)

# Nieuwe functies voor uploaden naar Supabase
def upload_projects_to_supabase(data: list[dict]):
    df = pd.DataFrame(data)
    df = sanitize_for_supabase(df)

    # Sanity-check en logging vóór upload
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    for col in df.columns:
        if df[col].dtype == float:
            if df[col].apply(lambda x: x is not None and (np.isnan(x) or np.isinf(x))).any():
                print(f"🚨 Probleem in kolom: {col}")

    # Fallback voor out-of-range floats vóór JSON serialisatie
    for col in df.columns:
        if df[col].dtype == float:
            df[col] = df[col].apply(lambda x: None if isinstance(x, float) and not np.isfinite(x) else x)
    cleaned_records = json.loads(json.dumps(df.replace([np.inf, -np.inf], None).to_dict("records"), default=str))
    res = supabase.table("projects").insert(cleaned_records).execute()
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

def upload_tasktypes_to_supabase(data: list[dict]):
    res = supabase.table("tasktypes").insert(data).execute()
    if res.status_code != 201:
        print("⚠️ Upload tasktypes mislukt:", res.json())
    else:
        print(f"✅ {len(data)} tasktypes geüpload naar Supabase")


POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

BASE_URL = "https://api.gripp.com/public/api3.php"
API_KEY = os.getenv("GRIPP_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
CACHE_PATH = "data/gripp_hours.parquet"
MAX_CACHE_AGE_MINUTES = 30
CACHE_DIR = "data"
os.makedirs(CACHE_DIR, exist_ok=True)


def is_cache_fresh():
    if not os.path.exists(CACHE_PATH):
        return False
    modified = datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
    return datetime.now() - modified < timedelta(minutes=MAX_CACHE_AGE_MINUTES)

def cached_fetch(name: str, fetch_fn: Callable[[], pd.DataFrame], force_refresh=False) -> pd.DataFrame:
    cache_path = f"data/{name}.parquet"
    if not force_refresh and os.path.exists(cache_path):
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
    return cached_fetch("gripp_projects", fetch, force_refresh=FORCE_REFRESH)

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
    return cached_fetch("gripp_employees", fetch, force_refresh=FORCE_REFRESH)

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
    return cached_fetch("gripp_companies", fetch, force_refresh=FORCE_REFRESH)

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
    return cached_fetch("gripp_hours_data", fetch, force_refresh=FORCE_REFRESH)

def fetch_gripp_costheadings():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "costheading.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
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
    return cached_fetch("gripp_costheadings", fetch, force_refresh=FORCE_REFRESH)

def fetch_gripp_costs():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "cost.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
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
    return cached_fetch("gripp_costs", fetch, force_refresh=FORCE_REFRESH)

def fetch_gripp_tasktypes():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: tasktypes geladen uit dummy bestand.")
            return pd.read_csv("mock_data/tasktypes.csv")
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "tasktype.get",
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
    return cached_fetch("gripp_tasktypes", fetch, force_refresh=FORCE_REFRESH)

# Toegevoegd: Ophalen van projectlijnen (offerprojectlines)
def fetch_gripp_projectlines():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "offerprojectline.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            time.sleep(0.5)
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            remaining = response.headers.get("X-RateLimit-Remaining")
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
    return cached_fetch("gripp_projectlines", fetch, force_refresh=FORCE_REFRESH)


if __name__ == "__main__":
    import pprint
    datasets = {}
    # Ophalen en sanitiseren van datasets
    print("⏳ Ophalen van projecten...")
    time.sleep(0.5)
    projects_raw = flatten_dict_column(fetch_gripp_projects())
    projects_clean = sanitize_for_supabase(projects_raw)
    datasets["gripp_projects"] = filter_projects(projects_clean)

    print("⏳ Ophalen van medewerkers...")
    time.sleep(0.5)
    employees_raw = flatten_dict_column(fetch_gripp_employees())
    employees_clean = sanitize_for_supabase(employees_raw)
    datasets["gripp_employees"] = filter_employees(employees_clean)

    print("⏳ Ophalen van relaties...")
    time.sleep(0.5)
    companies_raw = flatten_dict_column(fetch_gripp_companies())
    companies_clean = sanitize_for_supabase(companies_raw)
    datasets["gripp_companies"] = filter_companies(companies_clean)

    print("⏳ Ophalen van tasktypes...")
    time.sleep(0.5)
    tasktypes_raw = flatten_dict_column(fetch_gripp_tasktypes())
    tasktypes_clean = sanitize_for_supabase(tasktypes_raw)
    datasets["gripp_tasktypes"] = filter_tasktypes(tasktypes_clean)

    print("⏳ Ophalen van uren...")
    time.sleep(0.5)
    hours_raw = flatten_dict_column(fetch_gripp_hours_data())
    hours_clean = sanitize_for_supabase(hours_raw)
    datasets["gripp_hours_data"] = filter_hours(hours_clean)

    print("⏳ Ophalen van kostencategorieën (costheadings)...")
    costheadings_raw = flatten_dict_column(fetch_gripp_costheadings())
    costheadings_clean = sanitize_for_supabase(costheadings_raw)
    datasets["gripp_costheadings"] = costheadings_clean

    print("⏳ Ophalen van kosten (costs)...")
    costs_raw = flatten_dict_column(fetch_gripp_costs())
    costs_clean = sanitize_for_supabase(costs_raw)
    datasets["gripp_costs"] = costs_clean

    # Toegevoegd: Ophalen van projectlijnen (offerprojectlines)
    print("⏳ Ophalen van projectlijnen (offerprojectlines)...")
    projectlines_raw = flatten_dict_column(fetch_gripp_projectlines())
    projectlines_clean = sanitize_for_supabase(projectlines_raw)
    datasets["gripp_projectlines"] = projectlines_clean

    print(f"\n📊 Dataset naam: gripp_projectlines")
    print(f"  Aantal rijen: {len(projectlines_clean)}")
    print(f"  Kolomnamen: {projectlines_clean.columns.tolist()}")
    if not projectlines_clean.empty:
        import pprint
        pprint.pprint(projectlines_clean.head(5).to_dict(orient='records'), indent=2, width=120, compact=False)
    else:
        print("  Eerste record als dictionary: (dataset is leeg)")

    def print_project_details(project_id: int):
        project = datasets["gripp_projects"]
        projectlines = datasets["gripp_projectlines"]
        project_filtered = project[project["id"] == project_id]
        projectlines_filtered = projectlines[projectlines["offerprojectbase_id"] == project_id]

        print(f"\n📋 Project info (ID {project_id}):")
        if not project_filtered.empty:
            import pprint
            pprint.pprint(project_filtered.iloc[0].to_dict(), indent=2, width=120, compact=False)
        else:
            print("⚠️ Project niet gevonden.")

        print(f"\n🧾 Detailregels (offerprojectline) voor project {project_id} (aantal: {len(projectlines_filtered)}):")
        if not projectlines_filtered.empty:
            import pprint
            pprint.pprint(projectlines_filtered.to_dict(orient='records'), indent=2, width=120, compact=False)
        else:
            print("⚠️ Geen detailregels gevonden voor dit project.")

    print_project_details(273)  # Vervang 273 door de gewenste project-ID

    for name in ["gripp_costheadings", "gripp_costs"]:
        df = datasets[name]
        print(f"\n📊 Dataset naam: {name}")
        print(f"  Aantal rijen: {len(df)}")
        print(f"  Kolomnamen: {df.columns.tolist()}")
        if not df.empty:
            first_record = df.iloc[0].to_dict()
            import pprint
            pprint.pprint(first_record, indent=2, width=120, compact=False)
        else:
            print("  Eerste record als dictionary: (dataset is leeg)")

    print(f"\n🔑 API Key geladen: {'gevonden' if API_KEY else 'NIET gevonden'}")

    print_project_costs_and_hours(1089)  # Vervang 273 door de gewenste project-ID



    # Print per dataset de gevraagde info
    for name, df in datasets.items():
        print(f"\n📊 Dataset naam: {name}")
        print(f"  Aantal rijen: {len(df)}")
        print(f"  Kolomnamen: {df.columns.tolist()}")
        if not df.empty:
            first_record = df.iloc[6].to_dict()
            pprint.pprint(first_record, indent=2, width=120, compact=False)
        else:
            print("  Eerste record als dictionary: (dataset is leeg)")


    def find_invoices_by_project_number_and_company(project_number: int, company_name: str):
        projects_df = datasets["gripp_projects"]
        project_id = None
        for _, row in projects_df.iterrows():
            if row.get("number") == project_number and row.get("company_searchname") == company_name:
                project_id = row.get("id")
                break

        if project_id is None:
            print(f"⚠️ Geen project gevonden met nummer {project_number} voor {company_name}.")
            return None

        print(f"✅ Project-ID gevonden: {project_id}")

        projectlines_df = datasets["gripp_projectlines"]
        facturen = projectlines_df[projectlines_df["offerprojectbase_id"] == project_id]
        print(f"📄 Aantal factuurregels gevonden: {len(facturen)}")
        if not facturen.empty:
            import pprint
            pprint.pprint(facturen.to_dict(orient="records"), indent=2, width=120, compact=False)
        else:
            print("⚠️ Geen factuurregels gevonden voor dit project.")
        return facturen

    print("\n🔍 Facturen zoeken voor Opdracht 1089 en bedrijf 'Korff Dakwerken Volendam B.V.'")
    find_invoices_by_project_number_and_company(1089, "Korff Dakwerken Volendam B.V.")

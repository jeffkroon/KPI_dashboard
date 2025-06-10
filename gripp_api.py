import sys
import pandas as pd
import requests
import os
import time
import pprint
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


def filter_active_projects_only(projects_df: pd.DataFrame) -> pd.DataFrame:
    """Filtert alleen niet-gearchiveerde projecten (actief)."""
    return projects_df[projects_df["archived"] == False]

def get_active_projectlines_for_company(company_name: str) -> pd.DataFrame:
    print(f"\n🔍 Actieve projectlines ophalen voor bedrijf: '{company_name}'...")

    projects_df = datasets.get("gripp_projects")
    projectlines_df = datasets.get("gripp_projectlines")

    if projects_df is None or projectlines_df is None:
        print("❌ Vereiste datasets zijn niet geladen.")
        return pd.DataFrame()

    active_projects = filter_active_projects_only(projects_df)
    company_projects = active_projects[active_projects["company_searchname"] == company_name]
    if company_projects.empty:
        print(f"⚠️ Geen actieve projecten gevonden voor bedrijf '{company_name}'.")
        return pd.DataFrame()

    project_ids = company_projects["id"].tolist()
    matching_lines = projectlines_df[projectlines_df["offerprojectbase_id"].isin(project_ids)]

    print(f"✅ Gevonden: {len(matching_lines)} projectlines voor {len(project_ids)} actieve projecten.")
    return matching_lines


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
def print_projectlines_for_company(company_name: str, projects_df: pd.DataFrame, projectlines_df: pd.DataFrame):
    """
    Print een overzicht van alle projectlines voor alle projecten van een bepaalde company.
    """
    print(f"\n📋 Projectlines overzicht voor bedrijf: '{company_name}'")
    # Filter projecten van het bedrijf
    projects_for_company = projects_df[projects_df["company_searchname"] == company_name]
    if projects_for_company.empty:
        print(f"⚠️ Geen projecten gevonden voor bedrijf '{company_name}'.")
        return
    project_ids = projects_for_company["id"].tolist()
    relevant_projectlines = projectlines_df[projectlines_df["offerprojectbase_id"].isin(project_ids)]
    print(f"  Aantal projecten gevonden: {len(projects_for_company)}")
    print(f"  Aantal projectlines gevonden: {len(relevant_projectlines)}")
    if relevant_projectlines.empty:
        print("⚠️ Geen projectlines gevonden voor deze projecten.")
        return
    # Print overzicht per project
    for project_id, project in projects_for_company.iterrows():
        lines = relevant_projectlines[relevant_projectlines["offerprojectbase_id"] == project["id"]]
        if lines.empty:
            continue
        print(f"\n🔹 Project: {project['name']} (ID {project['id']}, Nummer {project.get('number', '-')})")
        # Selecteer een paar interessante kolommen als ze bestaan
        columns_to_show = [col for col in [
            "id", "description", "amount", "totalexclvat", "tasktype_searchname", "createdon_date", "updatedon_date"
        ] if col in lines.columns]
        # Print netjes met pandas
        print(lines[columns_to_show].to_string(index=False))
    print("\n✅ Overzicht projectlines voor bedrijf afgerond.")
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

    # Toegevoegd: Ophalen van projectlijnen (offerprojectlines)
    print("⏳ Ophalen van projectlijnen (offerprojectlines)...")
    projectlines_raw = flatten_dict_column(fetch_gripp_projectlines())
    projectlines_clean = sanitize_for_supabase(projectlines_raw)
    datasets["gripp_projectlines"] = projectlines_clean

    print(f"\n📊 Dataset naam: gripp_projectlines")
    print(f"  Aantal rijen: {len(projectlines_clean)}")
    print(f"  Kolomnamen: {projectlines_clean.columns.tolist()}")
    if not projectlines_clean.empty:
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
            pprint.pprint(project_filtered.iloc[0].to_dict(), indent=2, width=120, compact=False)
        else:
            print("⚠️ Project niet gevonden.")

        print(f"\n🧾 Detailregels (offerprojectline) voor project {project_id} (aantal: {len(projectlines_filtered)}):")
        if not projectlines_filtered.empty:
            pprint.pprint(projectlines_filtered.to_dict(orient='records'), indent=2, width=120, compact=False)
        else:
            print("⚠️ Geen detailregels gevonden voor dit project.")

    print_project_details(273)  # Vervang 273 door de gewenste project-ID
    print(f"\n🔑 API Key geladen: {'gevonden' if API_KEY else 'NIET gevonden'}")

    def find_invoices_by_project_number_and_company(project_number: int, company_name: str, projects_df: pd.DataFrame, projectlines_df: pd.DataFrame):
        """
        Zoek een project op basis van projectnummer en bedrijfsnaam,
        en geef alle bijbehorende projectlines (factuurregels) terug.
        """
        project = projects_df[
            (projects_df["number"] == project_number) &
            (projects_df["company_searchname"] == company_name)
        ]
        if project.empty:
            print(f"⚠️ Geen project gevonden met nummer {project_number} voor bedrijf '{company_name}'.")
            return pd.DataFrame()
        project_id = project.iloc[0]["id"]
        print(f"✅ Project gevonden: '{project.iloc[0]['name']}' (ID {project_id})")
        invoices = projectlines_df[projectlines_df["offerprojectbase_id"] == project_id]
        print(f"📄 Aantal factuurregels gevonden: {len(invoices)}")
        if not invoices.empty:
            print(invoices.to_string(index=False))
        else:
            print("⚠️ Geen factuurregels gevonden voor dit project.")
        return invoices

    # Voorbeeld: projectnummer 1089 en bedrijf 'Korff Dakwerken Volendam B.V.'
    find_invoices_by_project_number_and_company(
        1089,
        "Korff Dakwerken Volendam B.V.",
        datasets["gripp_projects"],
        datasets["gripp_projectlines"]
    )

    def print_matching_columns_for_company_project(project_number: int, company_name: str):
        projects_df = datasets["gripp_projects"]
        projectlines_df = datasets["gripp_projectlines"]
        project = projects_df[
            (projects_df["number"] == project_number) &
            (projects_df["company_searchname"] == company_name)
        ]
        if project.empty:
            print(f"⚠️ Geen project gevonden met nummer {project_number} voor bedrijf '{company_name}'.")
            return
        project_id = project.iloc[0]["id"]
        projectlines = projectlines_df[projectlines_df["offerprojectbase_id"] == project_id]
        print(f"\n📋 Project (gripp_projects) kolommen:")
        print(project.columns.tolist())
        print(f"\n🧾 Projectlines (gripp_projectlines) kolommen voor project ID {project_id}:")
        print(projectlines.columns.tolist())
        common_cols = set(project.columns).intersection(set(projectlines.columns))
        print(f"\n🔗 Overeenkomende kolommen:")
        for col in sorted(common_cols):
            print(f"- {col}")
        print(f"\n🔍 Waarden vergelijking voor gemeenschappelijke kolommen:")
        for col in sorted(common_cols):
            project_value = project.iloc[0][col]
            projectlines_values = projectlines[col].dropna().unique()
            if len(projectlines_values) == 0:
                print(f"  - Kolom '{col}': Alleen waarde in project: {project_value} (geen waarden in projectlines)")
                continue
            if all((project_value == val) or (pd.isna(project_value) and pd.isna(val)) for val in projectlines_values):
                print(f"  - Kolom '{col}': Waarden matchen exact tussen project en alle projectlines.")
            else:
                print(f"  - Kolom '{col}': Waarden verschillen!")
                print(f"    Project waarde   : {project_value}")
                print(f"    Projectlines waarden: {projectlines_values}")

    print_matching_columns_for_company_project(1089, "Korff Dakwerken Volendam B.V.")

    # Toegevoegd: Print overzicht van projectlines voor een heel bedrijf
    print_projectlines_for_company(
        "Korff Dakwerken Volendam B.V.",
        datasets["gripp_projects"],
        datasets["gripp_projectlines"]
    )
    
    # Print de eerste 10 offerprojectlines
    
def get_first_offerprojectlines():
    payload = [{
        "id": 1,
        "method": "offerprojectline.get",
        "params": [
            [
                {
                    "field": "offerprojectline.id",
                    "operator": "greaterequals",
                    "value": 1
                }
            ],
            {
                "paging": {
                    "firstresult": 0,
                    "maxresults": 10
                },
                "orderings": [
                    {
                        "field": "offerprojectline.id",
                        "direction": "asc"
                    }
                ]
            }
        ]
    }]

    response = requests.post(BASE_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()
    rows = data[0]["result"]["rows"]
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    return df


def get_projectlines_for_company(company_name: str) -> pd.DataFrame:
    print(f"\n🔍 Projectlines ophalen voor bedrijf: '{company_name}'...")

    # datasets moet globaal beschikbaar zijn
    projects_df = datasets.get("gripp_projects")
    projectlines_df = datasets.get("gripp_projectlines")

    if projects_df is None or projectlines_df is None:
        print("❌ Vereiste datasets zijn niet geladen.")
        return pd.DataFrame()

    # Stap 1: Filter projecten van dit bedrijf
    company_projects = projects_df[projects_df["company_searchname"] == company_name]
    if company_projects.empty:
        print(f"⚠️ Geen projecten gevonden voor bedrijf '{company_name}'.")
        return pd.DataFrame()

    project_ids = company_projects["id"].tolist()

    # Stap 2: Filter de offerprojectlines
    matching_lines = projectlines_df[projectlines_df["offerprojectbase_id"].isin(project_ids)]

    if matching_lines.empty:
        print(f"⚠️ Geen projectlines gevonden voor projecten van '{company_name}'.")
    else:
        print(f"✅ Gevonden: {len(matching_lines)} projectlines voor {len(project_ids)} projecten.")
        import pprint
        pprint.pprint(matching_lines.head(10).to_dict(orient="records"), indent=2)

    return matching_lines

def calculate_total_costs_per_task_type(projectlines: list) -> dict:
    """
    Berekent de totale kosten per 'product_searchname' (soort taak) op basis van de projectlines.
    Returnt een dictionary met taaknaam als key en totaalprijs als value.
    """
    from collections import defaultdict

    total_per_task = defaultdict(float)

    for line in projectlines:
        try:
            if line.get("rowtype_searchname") == "NORMAAL":
                task = line.get("product_searchname", "Onbekend")
                amount = float(line.get("amountwritten", 0))
                price = float(line.get("sellingprice", 0))
                total_per_task[task] += amount * price
        except (TypeError, ValueError):
            continue  # Foute data overslaan

    return dict(total_per_task)


def print_total_costs_per_tasktype_for_company(company_name: str):
    lines = get_active_projectlines_for_company(company_name)
    if lines.empty:
        print("❌ Geen projectlines beschikbaar voor analyse.")
        return

    kosten_per_taak = calculate_total_costs_per_task_type(lines.to_dict(orient="records"))
    kosten_df = pd.DataFrame([
        {"tasktype": taak, "total_cost": kosten}
        for taak, kosten in kosten_per_taak.items()
    ]).sort_values(by="total_cost", ascending=False)

    print(f"\n💰 Totale kosten per soort taak voor '{company_name}':")
    print(kosten_df.to_string(index=False))
    return kosten_df

# Aanroep

get_projectlines_for_company("Mijnijzerwaren B.V.")
print_total_costs_per_tasktype_for_company("Mijnijzerwaren B.V.")




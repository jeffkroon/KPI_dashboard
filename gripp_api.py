import sys
import numpy as np
import pandas as pd
import os
import time as pytime
import requests
import json
from datetime import datetime, timedelta, time
from dotenv import load_dotenv
from typing import Callable
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
import tempfile

# === Configuratieparameters ===
load_dotenv()
FORCE_REFRESH = "--refresh" in sys.argv
MOCK_MODE = False  # Zet op False voor live API-verzoeken
PROJECTLINES_CACHE_PATH = "data/projectlines_per_company.parquet"

BASE_URL = "https://api.gripp.com/public/api3.php"
GRIPP_API_KEY = os.getenv("GRIPP_API_KEY")
if not GRIPP_API_KEY:
    raise ValueError("GRIPP_API_KEY is not set in the environment.")
HEADERS = {"Authorization": f"Bearer {GRIPP_API_KEY}"}
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")
engine = create_engine(POSTGRES_URL)
CACHE_PATH = "data/gripp_hours.parquet"
MAX_CACHE_AGE_MINUTES = 30
CACHE_DIR = "data"
os.makedirs(CACHE_DIR, exist_ok=True)


datasets = {}

def filter_active_projects_only(projects_df: pd.DataFrame) -> pd.DataFrame:
    """Filtert alleen niet-gearchiveerde projecten (actief)."""
    return pd.DataFrame(projects_df[projects_df["archived"] == False].copy())


def get_projectlines_for_company(company_name: str) -> pd.DataFrame:
    print(f"\n🔍 Projectlines ophalen voor bedrijf: '{company_name}'...")

    projects_df = datasets.get("gripp_projects")
    projectlines_df = datasets.get("gripp_projectlines")

    if projects_df is None or projectlines_df is None:
        print("❌ Vereiste datasets zijn niet geladen.")
        return pd.DataFrame()

    company_projects = projects_df[projects_df["company_searchname"] == company_name]
    if company_projects.empty:
        print(f"⚠️ Geen projecten gevonden voor bedrijf '{company_name}'.")
        return pd.DataFrame()

    project_ids = company_projects["id"].tolist()
    matching_lines = projectlines_df[projectlines_df["offerprojectbase_id"].isin(project_ids)]

    # Filter op definitief en normal (zonder invoicebasis)
    matching_lines = matching_lines[
        (matching_lines["status_searchname"] == "DEFINITIEF") &
        (matching_lines["rowtype_searchname"] == "NORMAL")
    ]

    if matching_lines.empty:
        print(f"⚠️ Geen projectlines gevonden voor projecten van '{company_name}'.")
    else:
        print(f"✅ Gevonden: {len(matching_lines)} projectlines voor {len(project_ids)} projecten.")
        # Voor debugging: print eerste 10 regels (optioneel)
        print(matching_lines.head(10).to_dict(orient="records"))

    return matching_lines

def calculate_total_costs_per_task_type(projectlines: list) -> dict:
    # Helperfunctie voor het berekenen van totale kosten per taaktype
    from collections import defaultdict
    total_per_task = defaultdict(float)

    for line in projectlines:
        try:
            if (
                line.get("rowtype_searchname") == "NORMAL"
                and line.get("status_searchname") == "DEFINITIEF"
                and line.get("invoicebasis") == "FIXED"
            ):
                task = line.get("product_searchname", "Onbekend")
                amount = float(line.get("amountwritten", 0))
                price = float(line.get("sellingprice", 0))
                total_per_task[task] += amount * price
        except (TypeError, ValueError):
            continue

    return dict(total_per_task)


def print_total_costs_per_tasktype_for_company(company_name: str):
    lines = get_active_projectlines_for_company(company_name)
    if lines.empty:
        print("❌ Geen projectlines beschikbaar voor analyse.")
        return

    kosten_per_taak = calculate_total_costs_per_task_type(lines.to_dict(orient="records"))
    kosten_df = pd.DataFrame([
        {"tasktype": taak, "total_cost": round(float(kosten), 2)}
        for taak, kosten in kosten_per_taak.items()
    ]).sort_values(by="total_cost", ascending=False)

    print(f"\n💰 Totale kosten per soort taak voor '{company_name}':")
    print(kosten_df.to_string(index=False))
    return kosten_df


# Helper function for debugging unique values
def log_unique_values(df: pd.DataFrame, columns: list):
    for col in columns:
        if col in df.columns:
            print(f"Unieke waarden in '{col}': {df[col].unique()}\n")
        else:
            print(f"Kolom '{col}' bestaat niet in de dataset.\n")





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

    # Filter op definitief en normal (zonder invoicebasis)
    matching_lines = matching_lines[
        (matching_lines["status_searchname"] == "DEFINITIEF") &
        (matching_lines["rowtype_searchname"] == "NORMAL")
    ]

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
    return pd.DataFrame(df[cols].copy())

def filter_employees(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "firstname", "lastname", "searchname", "email", "function", "active",
        "employeesince_date", "department_id", "role_id", "updatedon_date", "identity_id"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return pd.DataFrame(df[cols].copy())

def filter_invoices(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id",
        "number",
        "subject",         # <-- toegevoegd
        "description",
        "date_date",
        "status_searchname",
        "totalinclvat",
        "totalexclvat",
        "company_id",
        "company_searchname",
        "client_id",
        "client_searchname",
        "identity_searchname",
        "totalpayed",
        "fase",
        "company",
        # "invoicelines",    # niet toevoegen
        "tags",
        "status"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return pd.DataFrame(df[cols].copy())

# === Toevoegen: filter_invoicelines functie (optioneel, kan worden aangepast) ===
def filter_invoicelines(df: pd.DataFrame) -> pd.DataFrame:
    # Kolomnamen zijn afhankelijk van Gripp API response voor invoicelines
    # Hier nemen we een ruime selectie, pas aan indien gewenst
    keep_cols = [
        "id",
        "invoice_id",
        "description",
        "amount",
        "price",
        "total",
        "product_id",
        "product_searchname",
        "createdon_date",
        "updatedon_date"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return pd.DataFrame(df[cols].copy())


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
    return pd.DataFrame(df[cols].copy())

def filter_tasktypes(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "id", "name", "searchname", "color", "createdon_date", "updatedon_date"
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return pd.DataFrame(df[cols].copy())

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
    return pd.DataFrame(df[cols].copy())
def flatten_dict_column(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        is_dict_series = df[col].apply(lambda x: isinstance(x, dict))
        if bool(is_dict_series.any()):
            expanded = df[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            df = df.drop(columns=[col]).join(expanded)
    return pd.DataFrame(df)

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

def post_with_rate_limit_handling(*args, **kwargs):
    """
    Doet een requests.post, checkt op rate limit headers en status 429, en pauzeert indien nodig tot tokens zijn hersteld.
    """
    while True:
        response = requests.post(*args, **kwargs)
        if response.status_code == 429:
            # Altijd wachten bij 429, ook als headers ontbreken
            reset_timestamp = response.headers.get("X-RateLimit-Reset")
            if reset_timestamp:
                reset_time = datetime.fromtimestamp(int(reset_timestamp))
                now = datetime.now()
                wait_seconds = (reset_time - now).total_seconds()
                if wait_seconds > 0:
                    print(f"⏸️ 429 Too Many Requests: wacht {int(wait_seconds)} seconden tot {reset_time.strftime('%H:%M:%S')}")
                    pytime.sleep(wait_seconds + 1)
                else:
                    print("⏸️ 429 Too Many Requests: reset tijd verstreken, probeer opnieuw...")
                    pytime.sleep(2)
            else:
                print("⏸️ 429 Too Many Requests: geen reset header, wacht 300 seconden (5 minuten) uit voorzorg...")
                pytime.sleep(300)
            continue  # Probeer opnieuw na wachten
        # Normale rate limit check (voorzichtigheidshalve)
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_timestamp = response.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            try:
                remaining_int = int(remaining)
            except Exception:
                remaining_int = 1
        else:
            remaining_int = 1
        if remaining_int == 0 and reset_timestamp is not None:
            reset_time = datetime.fromtimestamp(int(reset_timestamp))
            now = datetime.now()
            wait_seconds = (reset_time - now).total_seconds()
            if wait_seconds > 0:
                print(f"⏸️ Rate limit bereikt, wacht {int(wait_seconds)} seconden tot {reset_time.strftime('%H:%M:%S')}...")
                pytime.sleep(wait_seconds + 1)
            else:
                print("⏸️ Rate limit bereikt, maar reset tijd is verstreken. Probeer opnieuw...")
                pytime.sleep(2)
            continue  # Probeer opnieuw na wachten
        return response

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
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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


def fetch_gripp_invoices():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "invoice.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
    return cached_fetch("gripp_invoices", fetch, force_refresh=FORCE_REFRESH)

# === Toevoegen: fetch_gripp_invoicelines functie ===
def fetch_gripp_invoicelines():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "invoiceline.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
    return cached_fetch("gripp_invoicelines", fetch, force_refresh=FORCE_REFRESH)


def fetch_gripp_hours_data():
    def fetch():
        if MOCK_MODE:
            print("📦 MOCK: hours geladen uit dummy bestand.")
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
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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


def fetch_gripp_projectphases():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 50
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "projectphase.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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
    return cached_fetch("gripp_projectphases", fetch, force_refresh=FORCE_REFRESH)


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
        lines = pd.DataFrame(relevant_projectlines[relevant_projectlines["offerprojectbase_id"] == project["id"]])
        if hasattr(lines, 'empty') and lines.empty:  # type: ignore
            continue
        print(f"\n🔹 Project: {project['name']} (ID {project['id']}, Nummer {project.get('number', '-')})")
        columns_to_show = [col for col in [
            "id", "description", "amount", "totalexclvat", "tasktype_searchname", "createdon_date", "updatedon_date"
        ] if col in lines.columns]  # type: ignore
        print(lines[columns_to_show].to_string(index=False))  # type: ignore
    print("\n✅ Overzicht projectlines voor bedrijf afgerond.")
# Toegevoegd: Ophalen van projectlijnen (offerprojectlines)


def fetch_gripp_projectlines():
    def fetch():
        all_rows = []
        start = 0
        max_results = 100
        watchdog = 200
        while watchdog > 0:
            payload = [{
                "id": 1,
                "method": "offerprojectline.get",
                "params": [
                    [],
                    {"paging": {"firstresult": start, "maxresults": max_results}}
                ]
            }]
            pytime.sleep(0.1)
            response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
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


def safe_to_sql(df: pd.DataFrame, table_name: str):
    if df.empty:
        print(f"⚠️ Geen data om naar '{table_name}' te schrijven. Sla over.")
        return

    # Log en verwijder rijen zonder bedrijf_id indien aanwezig
    if "bedrijf_id" in df.columns:
        missing_bedrijf = df[df["bedrijf_id"].isna()]
        if not missing_bedrijf.empty:
            print(f"⚠️ {len(missing_bedrijf)} rows missen 'bedrijf_id'. ID's: {missing_bedrijf['id'].tolist()[:10]}...")
            df = df.dropna(subset=["bedrijf_id"])

    print(f"🚀 Bulk insert '{table_name}' via staging COPY, rows: {df.shape[0]}")
    
    # Gebruik een nieuwe engine voor elke operatie om SSL connection issues te voorkomen
    # Verhoog statement_timeout naar 10 minuten (600000 ms)
    if not POSTGRES_URL:
        raise ValueError("POSTGRES_URL is not set")
    temp_engine = create_engine(f"{POSTGRES_URL}?options=-c statement_timeout=600000", pool_pre_ping=True, pool_recycle=300)
    
    # Voor grote datasets (>10.000 records): verwerk in batches
    batch_size = 2000  # Verkleind van 5000 naar 2000 om timeouts te voorkomen
    if len(df) > batch_size:
        print(f"📦 Grote dataset gedetecteerd ({len(df)} records), verwerk in batches van {batch_size}")
        
        # Verwerk in batches
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size].copy()  # Maak een echte kopie
            print(f"📦 Verwerk batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1} ({len(batch_df)} records)")
            _process_batch(batch_df, table_name, temp_engine)
        
        print(f"✅ '{table_name}' up-to-date met batch processing.")
    else:
        # Kleine dataset: verwerk in één keer
        _process_batch(df, table_name, temp_engine)
    
    # Cleanup
    temp_engine.dispose()


def _process_batch(df: pd.DataFrame, table_name: str, temp_engine):
    """Helper functie om een batch data te verwerken."""
    # Forceer alle *_date kolommen naar datetime.date vóór export (geen string conversie)
    for col in df.columns:
        if col.endswith('_date') or col.endswith('on_date'):
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
        try:
            # ✅ Haal kolommen op uit staging table
            with temp_engine.begin() as conn:
                staging_table = f"{table_name}_staging"
                # Zorg dat staging table bestaat (LIKE main table)
                conn.execute(text(f"CREATE TABLE IF NOT EXISTS {staging_table} (LIKE {table_name} INCLUDING ALL);"))
                try:
                    result = conn.execute(text(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{staging_table}'
                        ORDER BY ordinal_position;
                    """))
                    db_columns = [row[0] for row in result]
                except Exception as e:
                    print(f"⚠️ Timeout bij ophalen kolomnamen van {staging_table}, gebruik DataFrame-kolommen als fallback: {e}")
                    db_columns = list(df.columns)

            # Forced alignment: voeg ontbrekende db_columns toe als None
            for col in db_columns:
                if col not in df.columns:
                    df[col] = None

            # Veilige fallback: expliciet bedrijf_id vullen met None indien niet aanwezig
            if "bedrijf_id" not in df.columns:
                df.loc[:, "bedrijf_id"] = None

            # Filter DataFrame kolommen en zorg voor juiste volgorde
            filtered_df = df[[col for col in db_columns if col in df.columns]]

            # Export naar CSV met float_format om .0 te verwijderen
            filtered_df.to_csv(tmp.name, index=False, header=True, float_format='%.0f')
            tmp.flush()

            # Nieuwe verbinding voor de COPY operatie
            with temp_engine.begin() as conn:
                # staging_table is al bepaald
                with open(tmp.name, 'r') as f:
                    conn.connection.cursor().copy_expert(f"COPY {staging_table} FROM STDIN WITH CSV HEADER", f)

                # Check of er een unieke constraint is op de id kolom
                has_unique_constraint = False
                try:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) 
                        FROM pg_constraint 
                        WHERE conrelid = '{table_name}'::regclass 
                        AND contype = 'u' 
                        AND pg_get_constraintdef(oid) LIKE '%id%';
                    """))
                    constraint_count = result.scalar()
                    has_unique_constraint = constraint_count is not None and constraint_count > 0
                except:
                    pass

                if has_unique_constraint or table_name in ["invoices", "urenregistratie"]:
                    # Gebruik ON CONFLICT als er een unieke constraint is of voor expliciet genoemde tabellen
                    columns = [col for col in filtered_df.columns if col != "id"]
                    set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
                    insert_cols = ", ".join(filtered_df.columns)
                    select_cols = ", ".join(filtered_df.columns)
                    
                    merge_sql = f'''
INSERT INTO {table_name} ({insert_cols})
SELECT {select_cols} FROM {staging_table}
ON CONFLICT (id) DO UPDATE SET {set_clause};
'''
                    conn.execute(text(merge_sql))
                    print(f"✅ '{table_name}' batch up-to-date met ON CONFLICT merge.")
                else:
                    if table_name == "urenregistratie" or table_name == "invoices":
                        # Direct COPY naar hoofdtabel zonder staging merge
                        with open(tmp.name, 'r') as f:
                            conn.connection.cursor().copy_expert(
                                f"COPY {table_name} FROM STDIN WITH CSV HEADER", f
                            )
                        print(f"✅ '{table_name}' batch direct geCOPY'd zonder staging merge.")
                    else:
                        # Voor normale tabellen: gebruik INSERT IGNORE, behalve voor projectlines_per_company
                        if table_name == "projectlines_per_company":
                            # Voor projectlines: overschrijf bestaande rijen met nieuwe company info
                            columns = [col for col in filtered_df.columns if col != "id"]
                            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
                            insert_cols = ", ".join(filtered_df.columns)
                            select_cols = ", ".join(filtered_df.columns)
                            insert_sql = f'''
INSERT INTO {table_name} ({insert_cols})
SELECT {select_cols} FROM {staging_table}
ON CONFLICT (id) DO UPDATE SET {set_clause};
'''
                            conn.execute(text(insert_sql))
                            print(f"✅ '{table_name}' batch up-to-date met ON CONFLICT UPDATE.")
                        else:
                            # Voor andere tabellen: gebruik INSERT IGNORE
                            insert_cols = ", ".join(filtered_df.columns)
                            select_cols = ", ".join(filtered_df.columns)
                            insert_sql = f'''
INSERT INTO {table_name} ({insert_cols})
SELECT {select_cols} FROM {staging_table}
ON CONFLICT DO NOTHING;
'''
                            conn.execute(text(insert_sql))
                            print(f"✅ '{table_name}' batch up-to-date met INSERT IGNORE.")
                
                # Staging table legen na succesvolle merge
                try:
                    # Probeer eerst TRUNCATE met timeout
                    conn.execute(text(f"SET statement_timeout = '30s'; TRUNCATE {staging_table};"))
                    print(f"✅ Staging table '{staging_table}' geleegd.")
                except Exception as e:
                    print(f"⚠️ TRUNCATE faalde, probeer DELETE: {e}")
                    try:
                        # Fallback: DELETE met timeout
                        conn.execute(text(f"SET statement_timeout = '30s'; DELETE FROM {staging_table};"))
                        print(f"✅ Staging table '{staging_table}' geleegd via DELETE.")
                    except Exception as e2:
                        print(f"⚠️ Kon staging table niet legen (niet kritiek): {e2}")
                        # Niet kritiek, staging wordt bij volgende run overschreven
                
        except Exception as e:
            print(f"❌ Fout bij uploaden van batch voor '{table_name}': {e}")
            raise
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

def convert_date_columns(df):
    for col in df.columns:
        if col.endswith('_date') or col.endswith('on_date'):
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Zet altijd om naar date (alleen de datum-component)
            df[col] = df[col].dt.date
    return df

def main():
    # Test PostgreSQL-verbinding
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Verbonden met Supabase PostgreSQL:", result.scalar())
    except Exception as e:
        print("❌ Fout bij verbinden met Supabase PostgreSQL:", e)

    # Ophalen en sanitiseren van datasets (alle fetch en clean eerst, vóór verwerking)
    projects_raw = fetch_gripp_projects()
    # Flatten dict-kolommen in projects_raw
    for col in projects_raw.columns:
        if projects_raw[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = projects_raw[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            projects_raw = projects_raw.drop(columns=[col]).join(expanded)
    print("[DEBUG] Eerste 3 projecten:")
    print(projects_raw.head(3).to_dict())
    employees_raw = fetch_gripp_employees()
    companies_raw = fetch_gripp_companies()
    tasktypes_raw = fetch_gripp_tasktypes()
    hours_raw = fetch_gripp_hours_data()

    # === FIX: Flatten de 'task' kolom in urenregistratie ===
    if 'task' in hours_raw.columns:
        print("🔧 Flattening 'task' data in urenregistratie...")
        hours_raw['task_id'] = hours_raw['task'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else None
        )
        hours_raw['task_searchname'] = hours_raw['task'].apply(
            lambda x: x.get('searchname') if isinstance(x, dict) else None
        )
    invoices_raw = fetch_gripp_invoices()
    # Forceer flatten voor alle kolommen die dicts kunnen bevatten
    for col in invoices_raw.columns:
        if invoices_raw[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = invoices_raw[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            invoices_raw = invoices_raw.drop(columns=[col]).join(expanded)
    # Kopieer date_date zoals voorheen
    if not invoices_raw.empty and 'date' in invoices_raw.columns:
        invoices_raw['date_date'] = invoices_raw['date']
        invoices_raw['date_date'] = pd.to_datetime(invoices_raw['date_date'], errors='coerce').dt.date
    #invoicelines_raw = fetch_gripp_invoicelines()

    # Eerst datasets in dictionary plaatsen zodat fetch_gripp_projectlines er toegang toe heeft
    # Flatten company data in projects voordat filtering
    if not projects_raw.empty and 'company' in projects_raw.columns:
        print("🔢 [DEBUG] Flattening company data in projects...")
        projects_raw['company_id'] = projects_raw['company'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else x
        )
        projects_raw['company_searchname'] = projects_raw['company'].apply(
            lambda x: x.get('searchname') if isinstance(x, dict) else x
        )
        print(f"🔢 [DEBUG] Sample company_id's: {projects_raw['company_id'].head(5).tolist()}")
        print(f"🔢 [DEBUG] Sample company_searchname's: {projects_raw['company_searchname'].head(5).tolist()}")
    
    datasets["gripp_projects"] = filter_projects(projects_raw)
    datasets["gripp_employees"] = filter_employees(employees_raw)
    datasets["gripp_companies"] = filter_companies(companies_raw)
    datasets["gripp_tasktypes"] = filter_tasktypes(tasktypes_raw)
    datasets["gripp_hours_data"] = filter_hours(hours_raw)
    datasets["gripp_invoices"] = filter_invoices(invoices_raw)
    
    # Nu projectlines ophalen (nadat projects beschikbaar zijn)
    projectlines_raw = fetch_gripp_projectlines()
    print(f"🔢 [DEBUG] Aantal projectlines direct uit API: {len(projectlines_raw)}")
    
    # Voeg bedrijfsinformatie toe aan projectlines
    if not projectlines_raw.empty:
        print(f"🔢 [DEBUG] Projectlines kolommen: {projectlines_raw.columns.tolist()}")
        
        # Gebruik offerprojectbase kolom (bevat dictionaries met ID's)
        if 'offerprojectbase' in projectlines_raw.columns:
            print(f"🔢 [DEBUG] Sample offerprojectbase waarden: {projectlines_raw['offerprojectbase'].head(5).tolist()}")
            
            # Extraheer ID's uit de dictionaries
            projectlines_raw['offerprojectbase_id'] = projectlines_raw['offerprojectbase'].apply(
                lambda x: x.get('id') if isinstance(x, dict) else x
            )
            print(f"🔢 [DEBUG] Sample offerprojectbase_id waarden: {projectlines_raw['offerprojectbase_id'].head(5).tolist()}")
            
            projectlines_raw = projectlines_raw.merge(
                datasets["gripp_projects"][["id", "company_id", "company_searchname"]],
                left_on="offerprojectbase_id",
                right_on="id",
                how="left",
                suffixes=("", "_project")
            )
            projectlines_raw = projectlines_raw.drop(columns=["id_project"])
            
            # Hernoem kolommen voor consistentie met database
            projectlines_raw = projectlines_raw.rename(columns={
                "company_id": "bedrijf_id",
                "company_searchname": "bedrijf_naam"
            })
            print(f"🔢 [DEBUG] Aantal projectlines na merge met bedrijfsinformatie: {len(projectlines_raw)}")
            print(f"🔢 [DEBUG] Sample bedrijf_id's: {projectlines_raw['bedrijf_id'].head(5).tolist()}")
        else:
            print("⚠️ offerprojectbase kolom niet gevonden")

    # === FIX: Flatten de 'unit' kolom in projectlines ===
    if 'unit' in projectlines_raw.columns:
        print("🔧 Flattening 'unit' data in projectlines...")
        projectlines_raw['unit_id'] = projectlines_raw['unit'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else None
        )
        projectlines_raw['unit_searchname'] = projectlines_raw['unit'].apply(
            lambda x: x.get('searchname') if isinstance(x, dict) else None
        )
    
    datasets["gripp_projectlines"] = projectlines_raw
    #datasets["gripp_invoicelines"] = filter_invoicelines(invoicelines_raw)

    # Gebruik direct de verrijkte projectlines uit de fetch
    combined_projectlines = datasets["gripp_projectlines"]

    # Converteer date kolommen vóór database-write
    if combined_projectlines is not None:
        combined_projectlines = convert_date_columns(combined_projectlines)
        print(f"🔢 [DEBUG] Aantal projectlines die naar de database gaan: {len(combined_projectlines.drop_duplicates(subset='id'))}")
        safe_to_sql(combined_projectlines.drop_duplicates(subset="id"), "projectlines_per_company")
    if datasets.get("gripp_projects") is not None:
        cleaned_projects = datasets["gripp_projects"].drop_duplicates(subset="id")
        print("[DEBUG] Voor safe_to_sql: eerste 10 projecten met phase_searchname:")
        print(cleaned_projects[['id', 'name', 'phase_searchname']].head(10))
        safe_to_sql(cleaned_projects, "projects")
    if datasets.get("gripp_employees") is not None:
        safe_to_sql(datasets["gripp_employees"].drop_duplicates(subset="id"), "employees")
    if datasets.get("gripp_companies") is not None:
        safe_to_sql(datasets["gripp_companies"].drop_duplicates(subset="id"), "companies")
    if datasets.get("gripp_tasktypes") is not None:
        safe_to_sql(datasets["gripp_tasktypes"].drop_duplicates(subset="id"), "tasktypes")
    if datasets.get("gripp_hours_data") is not None:
        hours_data = convert_date_columns(datasets["gripp_hours_data"].drop_duplicates(subset="id"))
        safe_to_sql(hours_data, "urenregistratie")
    
    if datasets.get("gripp_invoices") is not None:
        invoices_df = datasets["gripp_invoices"].drop_duplicates(subset="id").copy()

        # Zet geneste kolommen in JSON (veilige serialisatie)
        import numpy as np
        json_cols = ["tags"]
        def safe_json_serialize(x):
            if isinstance(x, str):
                return x
            elif isinstance(x, np.ndarray):
                return json.dumps(x.tolist())
            elif isinstance(x, (dict, list)):
                return json.dumps(x)
            elif pd.isnull(x):
                return None
            else:
                return str(x)
        for col in json_cols:
            if col in invoices_df.columns:
                invoices_df[col] = invoices_df[col].apply(safe_json_serialize)

        # Gebruik safe_to_sql voor consistente verwerking
        safe_to_sql(invoices_df, "invoices")
    #if datasets.get("gripp_invoicelines") is not None:
        #safe_to_sql(datasets["gripp_invoicelines"].drop_duplicates(subset="id"), "invoicelines")

    # Debug: inspecteer de inhoud en het type van de kolom 'phase_id' en 'phase_searchname'
    print("[DEBUG] Eerste 10 waarden van 'phase_id' en 'phase_searchname':")
    print(projects_raw[['phase_id', 'phase_searchname']].head(10))
    print("[DEBUG] Type-overzicht van 'phase_id':")
    print(projects_raw['phase_id'].apply(type).value_counts())
    # Verrijk projects_raw met phase_searchname (direct uit dict)
    if (not projects_raw.empty) and ('phase' in projects_raw.columns):
        projects_raw['phase_searchname'] = projects_raw['phase'].apply(
            lambda x: x.get('searchname') if isinstance(x, dict) else None
        )

if __name__ == "__main__":
    main()



import sys
import numpy as np
import pandas as pd
import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Callable
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect

# === Configuratieparameters ===
load_dotenv()
FORCE_REFRESH = "--refresh" in sys.argv
MOCK_MODE = False  # Zet op False voor live API-verzoeken
PROJECTLINES_CACHE_PATH = "data/projectlines_per_company.parquet"

BASE_URL = "https://api.gripp.com/public/api3.php"
API_KEY = os.getenv("GRIPP_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)
CACHE_PATH = "data/gripp_hours.parquet"
MAX_CACHE_AGE_MINUTES = 30
CACHE_DIR = "data"
os.makedirs(CACHE_DIR, exist_ok=True)


datasets = {}

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


def safe_to_sql(df: pd.DataFrame, table_name: str):
    insp = inspect(engine)
    if table_name in insp.get_table_names():
        print(f"🔁 Tabel {table_name} bestaat al — vervangen met nieuwe data.")
        df.to_sql(table_name, con=engine, if_exists="replace", index=False, method="multi")
    else:
        print(f"🆕 Nieuwe tabel aangemaakt: {table_name}")
        df.to_sql(table_name, con=engine, if_exists="replace", index=False, method="multi")


def main():
    # Test PostgreSQL-verbinding
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Verbonden met Supabase PostgreSQL:", result.scalar())
    except Exception as e:
        print("❌ Fout bij verbinden met Supabase PostgreSQL:", e)

    # Ophalen en sanitiseren van datasets (alle fetch en clean eerst, vóór verwerking)
    projects_raw = flatten_dict_column(fetch_gripp_projects())
    employees_raw = flatten_dict_column(fetch_gripp_employees())
    companies_raw = flatten_dict_column(fetch_gripp_companies())
    tasktypes_raw = flatten_dict_column(fetch_gripp_tasktypes())
    hours_raw = flatten_dict_column(fetch_gripp_hours_data())
    projectlines_raw = flatten_dict_column(fetch_gripp_projectlines())

    datasets["gripp_projects"] = filter_projects(projects_raw)
    datasets["gripp_employees"] = filter_employees(employees_raw)
    datasets["gripp_companies"] = filter_companies(companies_raw)
    datasets["gripp_tasktypes"] = filter_tasktypes(tasktypes_raw)
    datasets["gripp_hours_data"] = filter_hours(hours_raw)
    datasets["gripp_projectlines"] = projectlines_raw

    # Verzamel alle projectlines per bedrijf
    if os.path.exists(PROJECTLINES_CACHE_PATH) and not FORCE_REFRESH:
        combined_projectlines = pd.read_parquet(PROJECTLINES_CACHE_PATH)
    else:
        combined_projectlines = collect_projectlines_per_company(
            datasets["gripp_companies"],
            datasets["gripp_projects"],
            datasets["gripp_projectlines"]
        )
        combined_projectlines.to_parquet(PROJECTLINES_CACHE_PATH, index=False)

    # Upload alle datasets naar de database met veilige to_sql
    if datasets.get("gripp_projects") is not None:
        cleaned_projects = datasets["gripp_projects"].drop_duplicates(subset="id")
        safe_to_sql(cleaned_projects, "projects")
    if datasets.get("gripp_employees") is not None:
        safe_to_sql(datasets["gripp_employees"].drop_duplicates(subset="id"), "employees")
    if datasets.get("gripp_companies") is not None:
        safe_to_sql(datasets["gripp_companies"].drop_duplicates(subset="id"), "companies")
    if datasets.get("gripp_tasktypes") is not None:
        safe_to_sql(datasets["gripp_tasktypes"].drop_duplicates(subset="id"), "tasktypes")
    if datasets.get("gripp_hours_data") is not None:
        safe_to_sql(datasets["gripp_hours_data"].drop_duplicates(subset="id"), "urenregistratie")
    if combined_projectlines is not None:
        safe_to_sql(combined_projectlines.drop_duplicates(subset="id"), "projectlines_per_company")



if __name__ == "__main__":
    main()


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

    if matching_lines.empty:
        print(f"⚠️ Geen projectlines gevonden voor projecten van '{company_name}'.")
    else:
        print(f"✅ Gevonden: {len(matching_lines)} projectlines voor {len(project_ids)} projecten.")
        # Voor debugging: print eerste 10 regels (optioneel)
        print(matching_lines.head(10).to_dict(orient="records"))

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


def collect_projectlines_per_company(companies_df: pd.DataFrame, projects_df: pd.DataFrame, projectlines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Voor alle bedrijven: verzamel alle projectlines van hun projecten.
    Voeg bedrijfsnaam en bedrijfs-ID toe aan elke regel.
    """
    all_lines = []

    for _, company in companies_df.iterrows():
        company_name = company["companyname"]
        company_id = company["id"]

        projects = projects_df[projects_df["company_id"] == company_id]
        if projects.empty:
            continue

        project_ids = projects["id"].tolist()
        lines = projectlines_df[projectlines_df["offerprojectbase_id"].isin(project_ids)]
        if lines.empty:
            continue

        lines = lines.copy()
        lines["bedrijf_id"] = company_id
        lines["bedrijf_naam"] = company_name

        all_lines.append(lines)

    if not all_lines:
        print("⚠️ Geen projectlines gevonden voor enige bedrijven.")
        return pd.DataFrame()

    combined_df = pd.concat(all_lines, ignore_index=True)
    print(f"✅ Samengevoegde projectlines voor {len(combined_df)} regels.")
    return combined_df

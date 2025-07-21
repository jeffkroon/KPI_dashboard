import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils.data_loaders import load_data_df
from datetime import datetime, timedelta

# Data & omgeving setup
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")
engine = create_engine(POSTGRES_URL)

def test_data_loading():
    print("\n=== Start Data Loading Test ===")
    
    # Datasets laden
    print("\nLoading employees...")
    df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
    if not isinstance(df_employees, pd.DataFrame):
        df_employees = pd.concat(list(df_employees), ignore_index=True)
    df_employees['fullname'] = df_employees['firstname'] + " " + df_employees['lastname']
    print(f"✓ Loaded {len(df_employees)} employees")

    print("\nLoading projects...")
    df_projects = load_data_df("projects", columns=["id", "name", "company_id", "archived", "totalexclvat", "phase_searchname"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    print(f"✓ Raw projects: {len(df_projects)}")

    print("\nLoading companies...")
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    print(f"✓ Loaded {len(df_companies)} companies")
    
    print("\nLoading tasks...")
    df_tasks = load_data_df("tasks", columns=["id", "type"])
    if not isinstance(df_tasks, pd.DataFrame):
        df_tasks = pd.concat(list(df_tasks), ignore_index=True)
    
    # Debug task type information
    print("\nDEBUG: First 5 raw task types:")
    print(df_tasks['type'].head().to_string())
    
    # Fix type dictionary parsing
    def extract_tasktype_id(type_data):
        if pd.isna(type_data):
            return None
        if isinstance(type_data, dict):
            return type_data.get('id')
        if isinstance(type_data, str):
            try:
                import json
                data = json.loads(type_data)
                return data.get('id')
            except:
                return None
        return None
    
    df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
    print(f"\n✓ Loaded {len(df_tasks)} tasks")
    print("Sample tasktype_ids after fix:", df_tasks['tasktype_id'].head().tolist())
    print("Null tasktype_ids:", df_tasks['tasktype_id'].isnull().sum())

    print("\nLoading tasktypes...")
    df_tasktypes = load_data_df("tasktypes", columns=["id", "searchname"])
    if not isinstance(df_tasktypes, pd.DataFrame):
        df_tasktypes = pd.concat(list(df_tasktypes), ignore_index=True)
    print(f"✓ Loaded {len(df_tasktypes)} tasktypes")
    print("Sample tasktypes:", df_tasktypes[['id', 'searchname']].head().to_dict('records'))

    # Filter actieve projecten
    df_projects_filtered = df_projects[
        (df_projects["archived"] == False) & 
        (df_projects["phase_searchname"].isin(["Voorbereiding", "Uitvoering"]))
    ]
    print(f"\nFiltered projects: {len(df_projects_filtered)} (active, in preparation/execution)")

    # Test datum range
    start_date = pd.to_datetime("2023-01-01")
    end_date = pd.to_datetime("today")
    print(f"\nTesting date range: {start_date.date()} to {end_date.date()}")

    # Laad uren
    print("\nLoading hours...")
    df_uren = load_data_df(
        "urenregistratie", 
        columns=["id", "offerprojectbase_id", "employee_id", "task_id", "amount", "task_searchname", "date_date", "status_searchname"],
        where=f"status_searchname = 'Gefiatteerd' AND date_date::timestamp BETWEEN '{start_date}' AND '{end_date}'"
    )
    if not isinstance(df_uren, pd.DataFrame):
        df_uren = pd.concat(list(df_uren), ignore_index=True)
    print(f"✓ Loaded {len(df_uren)} hours")

    # Test merges
    print("\nTesting merges...")
    
    # 1. Uren -> Tasks
    df_uren_tasks = df_uren.merge(
        df_tasks[['id', 'tasktype_id']], 
        left_on='task_id', 
        right_on='id', 
        how='left'
    )
    print(f"After tasks merge: {len(df_uren_tasks)} rows")
    print(f"Null tasktype_ids: {df_uren_tasks['tasktype_id'].isnull().sum()}")

    # 2. + Tasktypes
    df_uren_tasks['tasktype_id'] = pd.to_numeric(df_uren_tasks['tasktype_id'], errors='coerce')
    df_uren_full = df_uren_tasks.merge(
        df_tasktypes.rename(columns={'id': 'tasktype_id', 'searchname': 'tasktype_general_name'}),
        on='tasktype_id',
        how='left'
    )
    print(f"After tasktypes merge: {len(df_uren_full)} rows")
    print(f"Null tasktype_general_names: {df_uren_full['tasktype_general_name'].isnull().sum()}")

    # 3. + Employees
    df_uren_full = df_uren_full.merge(
        df_employees[['id', 'fullname']], 
        left_on='employee_id', 
        right_on='id', 
        how='left',
        suffixes=('', '_employee')
    )
    print(f"After employees merge: {len(df_uren_full)} rows")
    print(f"Null employee names: {df_uren_full['fullname'].isnull().sum()}")

    # Test grouping
    print("\nTesting grouping...")
    df_uren_full['maand'] = pd.to_datetime(df_uren_full['date_date']).dt.to_period('M').astype(str)
    
    # Per maand/taaktype
    uren_per_maand_taak = df_uren_full.groupby(['maand', 'tasktype_general_name'])['amount'].sum().reset_index()
    print("\nSample hours per month/tasktype:")
    print(uren_per_maand_taak.head().to_string())

    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_data_loading() 
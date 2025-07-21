import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import json
import ast

def main():
    print("--- 1. Database connection ---")
    load_dotenv()
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        print("❌ ERROR: POSTGRES_URL is not set.")
        return
    engine = create_engine(POSTGRES_URL)
    print("✅ Connected.")

    # --- 2. Load all relevant tables ---
    print("\n--- 2. Loading tables ---")
    df_projects = pd.read_sql("SELECT id, name, company_id, archived, totalexclvat, phase_searchname FROM projects", engine)
    df_companies = pd.read_sql("SELECT id, companyname FROM companies", engine)
    df_employees = pd.read_sql("SELECT id, firstname, lastname FROM employees", engine)
    df_employees['fullname'] = df_employees['firstname'] + ' ' + df_employees['lastname']
    df_tasktypes = pd.read_sql("SELECT id, searchname FROM tasktypes", engine)
    # Debug: Raw tasks
    df_tasks_raw = pd.read_sql("SELECT id, type FROM tasks", engine)
    print("\nRaw df_tasks from database:")
    print(df_tasks_raw.head(10))
    print("Unique id values in df_tasks_raw:", df_tasks_raw['id'].unique())
    print("Unique type values in df_tasks_raw:", df_tasks_raw['type'].unique())
    df_tasks = df_tasks_raw.copy()
    # Parse tasktype_id from type (JSON or dict)
    def extract_tasktype_id(type_data):
        if pd.isna(type_data):
            return None
        if isinstance(type_data, str):
            try:
                data = ast.literal_eval(type_data)
                return data.get('id') if isinstance(data, dict) else None
            except:
                return None
        return type_data.get('id') if isinstance(type_data, dict) else None
    df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
    print("\nAfter extracting tasktype_id (before dropna):")
    print(df_tasks.head(10))
    print("Unique tasktype_id values:", df_tasks['tasktype_id'].unique())
    df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
    print("\nAfter dropna on tasktype_id:")
    print(df_tasks.head(10))
    print("Unique tasktype_id values after dropna:", df_tasks['tasktype_id'].unique())

    # --- 3. Find top 5 active projects with most recent approved hours ---
    print("\n--- 3. Finding top 5 active projects with recent approved hours ---")
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    query = f'''
    SELECT p.id, p.name, COUNT(u.id) as approved_hours_count
    FROM urenregistratie u
    JOIN projects p ON u.offerprojectbase_id = p.id
    WHERE u.status_searchname = 'Gefiatteerd' AND u.date_date >= '{start_date}'
    GROUP BY p.id, p.name
    ORDER BY approved_hours_count DESC
    LIMIT 5
    '''
    df_active_projects = pd.read_sql(query, engine)
    print(df_active_projects)
    active_project_ids = df_active_projects['id'].tolist()

    # --- 4. Get all relevant urenregistratie rows for those projects ---
    print("\n--- 4. Loading urenregistratie for those projects ---")
    query = f'''
    SELECT * FROM urenregistratie
    WHERE status_searchname = 'Gefiatteerd'
    AND offerprojectbase_id IN ({','.join(map(str, active_project_ids))})
    AND date_date >= '{start_date}'
    '''
    df_uren = pd.read_sql(query, engine)
    print(f"Loaded {len(df_uren)} rows from urenregistratie.")
    print(df_uren.head())

    # --- 5. Join all info: project, company, employee, task, tasktype ---
    print("\n--- 5. Joining all info ---")
    df_uren = df_uren.merge(df_projects.rename(columns={'id': 'project_id'}), left_on='offerprojectbase_id', right_on='project_id', how='left')
    df_uren = df_uren.merge(df_companies.rename(columns={'id': 'company_id'}), left_on='company_id', right_on='company_id', how='left')
    df_uren = df_uren.merge(df_employees.rename(columns={'id': 'employee_id'}), left_on='employee_id', right_on='employee_id', how='left')
    df_uren = df_uren.merge(df_tasks.rename(columns={'id': 'task_id'}), left_on='task_id', right_on='task_id', how='left')
    df_uren = df_uren.merge(df_tasktypes.rename(columns={'id': 'tasktype_id'}), left_on='tasktype_id', right_on='tasktype_id', how='left')
    print(df_uren.head())

    # --- Debug: Task linkage ---
    print("\nUnique task_id values in urenregistratie:", df_uren['task_id'].dropna().unique())
    print("\nFirst 10 rows of df_tasks:")
    print(df_tasks.head(10))
    print("\nUnique tasktype_id values in df_tasks:", df_tasks['tasktype_id'].dropna().unique())

    # --- 6. Aggregations ---
    print("\n--- 6. Aggregations ---")
    # a) Total hours per project
    print("\nTotal hours per project:")
    print(df_uren.groupby(['project_id', 'name'])['amount'].sum().reset_index().rename(columns={'amount': 'total_hours'}))
    # b) Total hours per employee
    print("\nTotal hours per employee:")
    print(df_uren.groupby(['employee_id', 'fullname'])['amount'].sum().reset_index().rename(columns={'amount': 'total_hours'}))
    # c) Total hours per task type
    print("\nTotal hours per task type:")
    print(df_uren.groupby(['tasktype_id', 'searchname'])['amount'].sum().reset_index().rename(columns={'amount': 'total_hours'}))
    # d) Total hours per employee per month per task type
    print("\nTotal hours per employee per month per task type:")
    df_uren['maand'] = pd.to_datetime(df_uren['date_date']).dt.to_period('M').astype(str)
    print(df_uren.groupby(['maand', 'employee_id', 'fullname', 'tasktype_id', 'searchname'])['amount'].sum().reset_index().rename(columns={'amount': 'total_hours'}).head(20))

if __name__ == "__main__":
    main() 
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import ast

def run_server_side_test():
    """
    A test script to replicate the server-side aggregation logic
    of the werkverdeling.py dashboard for local testing.
    """
    # --- 1. SETUP & DATABASE CONNECTION ---
    print("--- 1. Setting up database connection... ---")
    load_dotenv()
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        print("❌ ERROR: POSTGRES_URL is not set.")
        return
    engine = create_engine(POSTGRES_URL)
    print("✅ Database connection successful.")

    # --- 2. LOAD BASE DATA ---
    print("\n--- 2. Loading base dimension tables (employees, projects, tasks)... ---")
    df_employees = pd.read_sql("SELECT id, firstname, lastname FROM employees", engine)
    df_employees['fullname'] = df_employees['firstname'] + ' ' + df_employees['lastname']
    
    df_projects_raw = pd.read_sql("SELECT id, name, company_id FROM projects", engine)
    df_companies = pd.read_sql("SELECT id as company_id, companyname FROM companies", engine)
    df_projects = df_projects_raw.merge(df_companies, on='company_id')
    df_projects = df_projects.rename(columns={'id': 'project_id'})

    df_tasktypes = pd.read_sql("SELECT id as tasktype_id, searchname FROM tasktypes", engine)
    df_tasks_raw = pd.read_sql("SELECT id, type FROM tasks", engine)
    def extract_tasktype_id(type_data):
        if pd.isna(type_data): return None
        if isinstance(type_data, str):
            try:
                data = ast.literal_eval(type_data)
                return data.get('id')
            except: return None
        return type_data.get('id')
    df_tasks = df_tasks_raw.copy()
    df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
    df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
    df_tasks['tasktype_id'] = pd.to_numeric(df_tasks['tasktype_id'], downcast='integer', errors='coerce')
    df_tasks = df_tasks.merge(df_tasktypes, on='tasktype_id')
    df_tasks = df_tasks.rename(columns={'id': 'task_id', 'searchname': 'task_name'})
    print("✅ Base data loaded and processed.")

    # --- 3. DEFINE FILTERS ---
    print("\n--- 3. Defining test filters... ---")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    project_ids = [342, 3368, 3101, 751, 335] # Known active projects
    print(f"Date Range: {start_date.date()} to {end_date.date()}")
    print(f"Project IDs: {project_ids}")

    # --- 4. RUN SERVER-SIDE AGGREGATIONS ---
    print("\n--- 4. Running server-side aggregation queries... ---")
    date_filter = f"date_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"
    project_filter = f"offerprojectbase_id IN ({','.join(map(str, project_ids))})"

    # a) KPI Query
    kpi_query = f"SELECT SUM(amount) as total_hours, COUNT(DISTINCT employee_id) as active_employees, COUNT(DISTINCT task_id) as tasks_done FROM urenregistratie WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}"
    df_kpi = pd.read_sql(kpi_query, engine)
    print("\n✅ KPI Results:")
    print(df_kpi)

    # b) Employee Hours Query
    employee_query = f"SELECT employee_id, SUM(amount) as total_hours FROM urenregistratie WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter} GROUP BY employee_id"
    df_employee_agg = pd.read_sql(employee_query, engine)
    df_employee_hours = df_employee_agg.merge(df_employees, left_on='employee_id', right_on='id')
    print("\n✅ Employee Hours (Top 5):")
    print(df_employee_hours.sort_values('total_hours', ascending=False).head())

    # c) Task Hours Query
    task_query = f"SELECT task_id, SUM(amount) as total_hours FROM urenregistratie WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter} GROUP BY task_id"
    df_task_agg = pd.read_sql(task_query, engine)
    df_task_hours = df_task_agg.merge(df_tasks, on='task_id')
    print("\n✅ Task Hours (Top 5):")
    print(df_task_hours.sort_values('total_hours', ascending=False).head())

    # d) Detailed View Query (LIMIT 10 for testing)
    detail_query = f"""
    SELECT u.date_date, u.employee_id, u.offerprojectbase_id, u.task_id, u.amount, u.description
    FROM urenregistratie u
    WHERE u.status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
    ORDER BY u.date_date DESC
    LIMIT 10
    """
    df_detail_base = pd.read_sql(detail_query, engine)

    # Merge with pre-loaded dimension tables to get the names
    df_detail = df_detail_base.merge(df_employees, left_on='employee_id', right_on='id', how='left')
    df_detail = df_detail.merge(df_projects, left_on='offerprojectbase_id', right_on='project_id', how='left')
    df_detail = df_detail.merge(df_tasks, on='task_id', how='left')

    # Select and rename the final columns for display
    df_display = df_detail[[
        'date_date', 'fullname', 'name', 'task_name', 'amount', 'description'
    ]].rename(columns={'name': 'project_name'})

    print("\n✅ Detailed View (Sample of 10):")
    print(df_display)
    print("\n--- Test Complete ---")

if __name__ == "__main__":
    run_server_side_test() 
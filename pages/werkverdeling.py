import streamlit as st
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import plotly.express as px
import numpy as np
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS
from utils.data_loaders import load_data_df
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Werkverdeling",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- AUTHENTICATION ---
require_login()
require_email_whitelist(ALLOWED_EMAILS)

if "access_token" in st.session_state:
    st.sidebar.write(f"Ingelogd als: {st.session_state.get('user_email', '')}")
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

st.logo("images/dunion-logo-def_donker-06.png")

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_engine():
    load_dotenv()
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        st.error("POSTGRES_URL is not set.")
        st.stop()
    return create_engine(POSTGRES_URL)

engine = get_engine()

# --- DATA LOADING AND CACHING ---
@st.cache_data(ttl=300)
def load_base_data():
    """Loads essential, non-time-series data like employees, projects, etc."""
    try:
        df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
        df_employees['fullname'] = df_employees['firstname'] + " " + df_employees['lastname']

        df_projects = load_data_df("projects", columns=["id", "name", "company_id", "archived", "totalexclvat", "phase_searchname"])
        df_projects = df_projects[(df_projects["archived"] == False) & (df_projects["phase_searchname"].isin(["Voorbereiding", "Uitvoering"]))].copy()
        df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")

        df_companies = load_data_df("companies", columns=["id", "companyname"])
        df_projects = df_projects.merge(df_companies[['id', 'companyname']], left_on='company_id', right_on='id', how='left')

        df_tasktypes = load_data_df("tasktypes", columns=["id", "searchname"])
        
        # Process tasks to get tasktype_id
        df_tasks = load_data_df("tasks", columns=["id", "type"])
        def extract_tasktype_id(type_data):
            if pd.isna(type_data): return None
            if isinstance(type_data, str):
                try:
                    data = eval(type_data)
                    return data.get('id') if isinstance(data, dict) else None
                except: return None
            return type_data.get('id') if isinstance(type_data, dict) else None
        df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
        df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
        df_tasks['tasktype_id'] = pd.to_numeric(df_tasks['tasktype_id'], downcast='integer', errors='coerce')

        return df_employees, df_projects, df_tasktypes, df_tasks

    except Exception as e:
        st.error(f"Error loading base data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def get_aggregated_hours(table, start_date, end_date, project_ids=None, employee_ids=None):
    """
    Fetches and aggregates hours data directly from the database,
    avoiding loading the full raw data into memory.
    """
    date_filter = f"date_date BETWEEN '{start_date}' AND '{end_date}'"
    project_filter = f"AND offerprojectbase_id IN ({','.join(map(str, project_ids))})" if project_ids else ""
    employee_filter = f"AND employee_id IN ({','.join(map(str, employee_ids))})" if employee_ids else ""
    
    query = f"""
    SELECT {table}
    FROM urenregistratie
    WHERE status_searchname = 'Gefiatteerd'
    AND {date_filter}
    {project_filter}
    {employee_filter}
    GROUP BY 1
    ORDER BY 1
    """
    return pd.read_sql(query, engine)

# --- INITIAL DATA LOAD ---
with st.spinner("Loading base data..."):
    df_employees, df_projects, df_tasktypes, df_tasks = load_base_data()

if df_employees.empty or df_projects.empty:
    st.error("Could not load essential data. Dashboard cannot continue.")
    st.stop()

# --- MAIN UI ---
st.title("📋 Opdracht Overzicht met Medewerker Uren")

# --- FILTERS ---
max_date = datetime.today()
min_date_default = max_date - timedelta(days=30)
date_range = st.date_input(
    "Selecteer datumrange urenregistratie",
    (min_date_default, max_date),
    min_value=datetime(2023, 1, 1),
    max_value=max_date,
    help="Default is last 30 days. Adjust for a different period."
)
start_date, end_date = date_range

# After merging with companies, use the correct column for project id
project_options = df_projects[['id_x', 'name']].rename(columns={'id_x': 'id'}).to_dict('records')
default_projects = project_options[:10]
select_all_projects = st.checkbox("Selecteer alle opdrachten", value=False)
selected_projects = project_options if select_all_projects else st.multiselect(
    "Selecteer één of meerdere opdrachten",
    options=project_options,
    default=default_projects,
    format_func=lambda x: f"{x['name']} (ID: {x['id']})"
)
project_ids = [p['id'] for p in selected_projects]

# --- DATA AGGREGATION (DATABASE-SIDE) ---
if project_ids:
    with st.spinner("Aggregating data..."):
        # KPI: Totale uren en medewerkers
        df_total_hours_per_employee = get_aggregated_hours(
            "employee_id, SUM(amount) as total_hours",
            start_date, end_date, project_ids
        )
        
        # Join with employee data in Pandas (small operation)
        df_total_hours_per_employee = df_total_hours_per_employee.merge(
            df_employees, left_on='employee_id', right_on='id'
        )

    # --- KPIs ---
    aantal_projecten = len(project_ids)
    totale_omzet = df_projects[df_projects['id'].isin(project_ids)]['totalexclvat'].sum()
    aantal_medewerkers = len(df_total_hours_per_employee)
    totale_uren = df_total_hours_per_employee['total_hours'].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Aantal geselecteerde opdrachten", aantal_projecten)
    col2.metric("Totale projectomzet (excl. btw)", f"€ {totale_omzet:,.2f}")
    col3.metric("Aantal medewerkers betrokken", aantal_medewerkers)
    col4.metric("Totale uren geschreven", f"{totale_uren:.2f}")

    # --- Medewerker Details Table ---
    st.subheader("👷 Medewerkers die aan geselecteerde opdrachten werken")
    if not df_total_hours_per_employee.empty:
        st.dataframe(df_total_hours_per_employee[['fullname', 'total_hours']].rename(columns={
            'fullname': 'Medewerker', 'total_hours': 'Uren aan selectie'
        }).sort_values('Uren aan selectie', ascending=False), use_container_width=True)
    else:
        st.info("Geen uren gevonden voor deze selectie.")

    # --- Visualizations ---
    st.subheader("📊 Visualisaties")
    if not df_total_hours_per_employee.empty:
        fig1 = px.bar(
            df_total_hours_per_employee.sort_values('total_hours', ascending=True),
            x='total_hours', y='fullname', orientation='h',
            title='Uren per medewerker', color='fullname', text_auto=True
        )
        fig1.update_layout(showlegend=False, yaxis_title="Medewerker", xaxis_title="Totaal Uren")
        st.plotly_chart(fig1, use_container_width=True)

        # For task-based charts, we need to load that specific aggregation
        with st.spinner("Aggregating task data..."):
            query = f"""
            SELECT task_id, SUM(amount) as total_hours
            FROM urenregistratie
            WHERE status_searchname = 'Gefiatteerd'
            AND date_date BETWEEN '{start_date}' AND '{end_date}'
            AND offerprojectbase_id IN ({','.join(map(str, project_ids))})
            GROUP BY task_id
            """
            df_hours_per_task = pd.read_sql(query, engine)
            
            # Now join with processed task data in Pandas
            df_hours_per_task = df_hours_per_task.merge(df_tasks, left_on='task_id', right_on='id')
            df_hours_per_task = df_hours_per_task.merge(df_tasktypes, left_on='tasktype_id', right_on='id')
            df_hours_per_task = df_hours_per_task.groupby('searchname')['total_hours'].sum().reset_index()

        df_taak = df_hours_per_task.sort_values('total_hours', ascending=False).head(10)
        fig2 = px.bar(
            df_taak, x='searchname', y='total_hours',
            title='Top 10 taken per uren', color='searchname', text_auto=True
        )
        fig2.update_layout(showlegend=False, xaxis_title='Taaktype', yaxis_title='Uren')
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("Selecteer één of meerdere opdrachten om de details te zien.")

st.markdown("---")
st.subheader("Analyse over alle opdrachten")

# Employee selection for global analysis
alle_medewerkers = df_employees['fullname'].dropna().unique().tolist()
geselecteerde_medewerkers = st.multiselect(
    "Selecteer medewerker(s) voor detailoverzicht",
    options=alle_medewerkers,
    default=alle_medewerkers[:3] if len(alle_medewerkers) > 3 else alle_medewerkers
)
employee_ids_filter = df_employees[df_employees['fullname'].isin(geselecteerde_medewerkers)]['id'].tolist() if geselecteerde_medewerkers else []

if employee_ids_filter:
    with st.spinner("Laden van medewerker details..."):
        query = f"""
        SELECT to_char(date_date, 'YYYY-MM') as maand, employee_id, task_id, SUM(amount) as total_hours
        FROM urenregistratie
        WHERE status_searchname = 'Gefiatteerd'
        AND date_date BETWEEN '{start_date}' AND '{end_date}'
        AND employee_id IN ({','.join(map(str, employee_ids_filter))})
        GROUP BY 1, 2, 3
        """
        df_detail = pd.read_sql(query, engine)

        # Join with all mappings in pandas
        df_detail = df_detail.merge(df_employees, left_on='employee_id', right_on='id')
        df_detail = df_detail.merge(df_tasks, left_on='task_id', right_on='id')
        df_detail = df_detail.merge(df_tasktypes, left_on='tasktype_id', right_on='id')
        
        # Aggregate again after joins
        df_detail_agg = df_detail.groupby(['maand', 'fullname', 'searchname'])['total_hours'].sum().reset_index()

    fig_detail = px.bar(
        df_detail_agg,
        x='maand', y='total_hours', color='searchname',
        facet_row='fullname', title='Uren per taaktype per maand per medewerker'
    )
    fig_detail.update_layout(barmode='stack', legend_title='Taaktype', yaxis_title='Totaal uren')
    st.plotly_chart(fig_detail, use_container_width=True)
else:
    st.info("Selecteer medewerkers voor een gedetailleerd overzicht.")

st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)
    
    

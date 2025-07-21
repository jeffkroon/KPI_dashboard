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
import ast

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

# --- DATA LOADING (from testfile.py logic) ---
@st.cache_data(ttl=300)
def load_base_data():
    """Loads all the non-timesheet dimension tables."""
    df_employees = pd.read_sql("SELECT id, firstname, lastname FROM employees", engine)
    df_employees['fullname'] = df_employees['firstname'] + ' ' + df_employees['lastname']
    
    df_projects = pd.read_sql("SELECT id, name, company_id, archived, totalexclvat, phase_searchname FROM projects", engine)
    df_companies = pd.read_sql("SELECT id, companyname FROM companies", engine)
    df_projects = df_projects.merge(df_companies, left_on='company_id', right_on='id', suffixes=('_proj', '_comp'))
    df_projects = df_projects.rename(columns={'id_proj': 'project_id'})

    df_tasktypes = pd.read_sql("SELECT id, searchname FROM tasktypes", engine)
    df_tasks_raw = pd.read_sql("SELECT id, type FROM tasks", engine)

    def extract_tasktype_id(type_data):
        if pd.isna(type_data): return None
        if isinstance(type_data, str):
            try:
                data = ast.literal_eval(type_data)
                return data.get('id') if isinstance(data, dict) else None
            except (ValueError, SyntaxError): return None
        return type_data.get('id') if isinstance(type_data, dict) else None
    
    df_tasks = df_tasks_raw.copy()
    df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
    df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
    df_tasks['tasktype_id'] = pd.to_numeric(df_tasks['tasktype_id'], downcast='integer', errors='coerce')

    return df_employees, df_projects, df_tasktypes, df_tasks

# --- INITIAL DATA LOAD ---
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
    help="Default is last 30 days."
)
# Fix for ValueError
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date_default, max_date

project_options = df_projects[['project_id', 'name']].to_dict('records')
known_active_project_ids = [342, 3368, 3101, 751, 335] 
default_projects = [p for p in project_options if p['project_id'] in known_active_project_ids]
if not default_projects:
    default_projects = project_options[:5]

select_all_projects = st.checkbox("Selecteer alle opdrachten", value=False)
selected_projects = project_options if select_all_projects else st.multiselect(
    "Selecteer één of meerdere opdrachten",
    options=project_options,
    default=default_projects,
    format_func=lambda x: f"{x['name']} (ID: {x['project_id']})"
)
project_ids = [p['project_id'] for p in selected_projects]

# --- DYNAMIC DATA LOADING AND PROCESSING (from testfile.py logic) ---
if project_ids:
    with st.spinner("Laden en verwerken van uren..."):
        # 1. Load raw hours for selected projects and date range
        query = f"""
        SELECT * FROM urenregistratie
        WHERE status_searchname = 'Gefiatteerd'
        AND offerprojectbase_id IN ({','.join(map(str, project_ids))})
        AND date_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
        """
        df_uren = pd.read_sql(query, engine)

        # 2. Join all info
        if not df_uren.empty:
            df_uren = df_uren.merge(df_projects, left_on='offerprojectbase_id', right_on='project_id', how='left')
            df_uren = df_uren.merge(df_employees, left_on='employee_id', right_on='id', suffixes=('_hour', '_emp'), how='left')
            df_uren = df_uren.merge(df_tasks, left_on='task_id', right_on='id', suffixes=('_hour', '_task'), how='left')
            df_uren = df_uren.merge(df_tasktypes, left_on='tasktype_id', right_on='id', suffixes=('_hour', '_tasktype'), how='left')
        else:
            st.info("Geen uren gevonden voor de geselecteerde projecten en datumperiode.")

    # --- KPIs ---
    if not df_uren.empty:
        aantal_projecten = len(project_ids)
        totale_omzet = df_projects[df_projects['project_id'].isin(project_ids)]['totalexclvat'].sum()
        aantal_medewerkers = df_uren['employee_id'].nunique()
        totale_uren = df_uren['amount'].sum()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Geselecteerde opdrachten", aantal_projecten)
        col2.metric("Projectomzet (excl. btw)", f"€ {totale_omzet:,.2f}")
        col3.metric("Medewerkers betrokken", aantal_medewerkers)
        col4.metric("Totaal uren geschreven", f"{totale_uren:.2f}")

        # --- Medewerker Details Table ---
        st.subheader("👷 Medewerkers die aan geselecteerde opdrachten werken")
        df_medewerkers_agg = df_uren.groupby(['employee_id', 'fullname'])['amount'].sum().reset_index()
        st.dataframe(df_medewerkers_agg.rename(columns={
            'fullname': 'Medewerker', 'amount': 'Uren aan selectie'
        }).sort_values('Uren aan selectie', ascending=False), use_container_width=True)

        # --- Visualizations ---
        st.subheader("📊 Visualisaties")
        # Hours per employee
        fig1 = px.bar(
            df_medewerkers_agg.sort_values('amount', ascending=True),
            x='amount', y='fullname', orientation='h',
            title='Uren per medewerker', color='fullname', text_auto=True
        )
        fig1.update_layout(showlegend=False, yaxis_title="Medewerker", xaxis_title="Totaal Uren")
        st.plotly_chart(fig1, use_container_width=True)

        # Hours per task type
        df_task_agg = df_uren.groupby(['tasktype_id', 'searchname'])['amount'].sum().reset_index()
        df_taak = df_task_agg.sort_values('amount', ascending=False).head(10)
        fig2 = px.bar(
            df_taak, x='searchname', y='amount',
            title='Top 10 taken per uren', color='searchname', text_auto=True
        )
        fig2.update_layout(showlegend=False, xaxis_title='Taaktype', yaxis_title='Uren')
        st.plotly_chart(fig2, use_container_width=True)

        # --- Detailed analysis section ---
        st.markdown("---")
        st.subheader("Analyse per Medewerker")
        alle_medewerkers_in_selectie = df_uren['fullname'].dropna().unique().tolist()
        geselecteerde_medewerkers = st.multiselect(
            "Filter op medewerker(s) in de selectie",
            options=alle_medewerkers_in_selectie,
            default=alle_medewerkers_in_selectie
        )
        if geselecteerde_medewerkers:
            df_detail = df_uren[df_uren['fullname'].isin(geselecteerde_medewerkers)].copy()
            df_detail['maand'] = pd.to_datetime(df_detail['date_date']).dt.to_period('M').astype(str)
            df_detail_agg = df_detail.groupby(['maand', 'fullname', 'searchname'])['amount'].sum().reset_index()

            fig_detail = px.bar(
                df_detail_agg,
                x='maand', y='amount', color='searchname',
                facet_row='fullname', title='Uren per taaktype per maand per medewerker'
            )
            fig_detail.update_layout(barmode='stack', legend_title='Taaktype', yaxis_title='Totaal uren', xaxis_title="Maand")
            st.plotly_chart(fig_detail, use_container_width=True)
else:
    st.info("Selecteer één of meerdere opdrachten om de details te zien.")

st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)
    

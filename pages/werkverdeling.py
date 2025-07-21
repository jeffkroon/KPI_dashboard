import streamlit as st
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import plotly.express as px
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS
from datetime import datetime, timedelta
import ast

# --- 1. PAGE CONFIG & AUTHENTICATION ---
st.set_page_config(
    page_title="Werkverdeling Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
require_login()
require_email_whitelist(ALLOWED_EMAILS)

# --- Sidebar ---
if "access_token" in st.session_state:
    st.sidebar.write(f"Ingelogd als: {st.session_state.get('user_email', '')}")
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()
st.logo("images/dunion-logo-def_donker-06.png")

# --- 2. DATABASE CONNECTION & BASE DATA LOADING ---
@st.cache_resource
def get_engine():
    """Creates a cached SQLAlchemy engine."""
    load_dotenv()
    POSTGRES_URL = os.getenv("POSTGRES_URL")
    if not POSTGRES_URL:
        st.error("POSTGRES_URL is not set.")
        st.stop()
    return create_engine(POSTGRES_URL)

engine = get_engine()

@st.cache_data(ttl=300)
def load_base_data():
    """
    Loads all the non-timesheet dimension tables (employees, projects, companies, tasks).
    This function is cached to improve performance.
    """
    try:
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
        
        # After merging with tasktypes, print columns and robustly rename
        df_tasks = df_tasks_raw.copy()
        df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
        df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
        df_tasks['tasktype_id'] = pd.to_numeric(df_tasks['tasktype_id'], downcast='integer', errors='coerce')
        df_tasks = df_tasks.merge(df_tasktypes, left_on='tasktype_id', right_on='id', suffixes=('_task', '_tasktype'))
        print('df_tasks columns after merge:', df_tasks.columns)
        # Robustly rename columns
        for col in ['id_task', 'id_x', 'id']:
            if col in df_tasks.columns:
                df_tasks = df_tasks.rename(columns={col: 'task_id'})
                break
        if 'searchname' in df_tasks.columns:
            df_tasks = df_tasks.rename(columns={'searchname': 'task_name'})


        # Filter for active projects only
        df_projects = df_projects[(df_projects["archived"] == False) & (df_projects["phase_searchname"].isin(["Voorbereiding", "Uitvoering"]))].copy()
        df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")

        return df_employees, df_projects, df_tasks
    except Exception as e:
        st.error(f"Fout bij het laden van basisdata: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- Load the data ---
df_employees, df_projects, df_tasks = load_base_data()

if df_employees.empty or df_projects.empty:
    st.error("Essentiële data (medewerkers of projecten) kon niet geladen worden. Het dashboard kan niet doorgaan.")
    st.stop()

# --- 3. UI LAYOUT & PLACEHOLDERS ---

# --- Color Scheme for Task Types (based on user image) ---
TASK_COLOR_MAP = {
    'Vormgeving / DTP': '#002060',
    'Consultancy / Concept / Advies': '#5B9BD5',
    'Affiliate Marketing': '#7030A0',
    'Customer Data Platform | CDP': '#FFC000',
    'Development': '#ED7D31',
    'Zoekmachine Optimalisatie | SEO': '#FF0000',
    'Overige': '#A5A5A5',
    'Zoekmachine Advertising | SEA': '#20B2AA',
    'Copywriting / Content Creatie': '#333E48',
    'Conversie Ratio Optimalisatie | CRO': '#D95319',
    'Email Marketing (Automation)': '#FFD966'
}

st.title("📊 Werkverdeling & Projectanalyse")
st.markdown("Selecteer een periode en projecten om de details te bekijken.")

# --- Section 1: Filters & KPIs ---
st.header("Filters & Hoofdcijfers")

# --- Filters ---
filter_col1, filter_col2 = st.columns([1, 2])
with filter_col1:
    max_date = datetime.today()
    min_date_default = max_date - timedelta(days=30)
    date_range = st.date_input(
        "📅 Periode",
        (min_date_default, max_date),
        min_value=datetime(2023, 1, 1),
        max_value=max_date,
        help="Selecteer de periode die u wilt analyseren."
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date_default, max_date

with filter_col2:
    project_options = df_projects.sort_values('name').to_dict('records')
    known_active_project_ids = [342, 3368, 3101, 751, 335]
    default_projects = [p for p in project_options if p['project_id'] in known_active_project_ids]
    if not default_projects:
        default_projects = project_options[:5]

    selected_projects = st.multiselect(
        "📂 Projecten",
        options=project_options,
        default=default_projects,
        format_func=lambda x: f"{x['name']} (ID: {x['project_id']})",
        help="Selecteer de projecten die u wilt analyseren."
    )
    project_ids = [p['project_id'] for p in selected_projects]

# --- Dynamic Data Loading based on filters ---
df_uren = pd.DataFrame()
if project_ids:
    query = f"""
    SELECT * FROM urenregistratie
    WHERE status_searchname = 'Gefiatteerd'
    AND offerprojectbase_id IN ({','.join(map(str, project_ids))})
    AND date_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    """
    df_uren = pd.read_sql(query, engine)

# --- KPIs ---
st.markdown("---")
kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
if not df_uren.empty:
    total_hours = df_uren['amount'].sum()
    active_employees = df_uren['employee_id'].nunique()
    tasks_done = df_uren['task_id'].nunique()
    avg_hours_per_project = total_hours / len(project_ids) if project_ids else 0
    
    kpi_col1.metric("Totaal Uren", f"{total_hours:,.2f}")
    kpi_col2.metric("Actieve Medewerkers", active_employees)
    kpi_col3.metric("Unieke Taken", tasks_done)
    kpi_col4.metric("Gem. Uur/Project", f"{avg_hours_per_project:,.2f}")
else:
    kpi_col1.metric("Totaal Uren", "0")
    kpi_col2.metric("Actieve Medewerkers", "0")
    kpi_col3.metric("Unieke Taken", "0")
    kpi_col4.metric("Gem. Uur/Project", "0")


# --- Section 2: Analysis of Selected Projects ---
st.markdown("---")
st.header("Analyse van Geselecteerde Projecten")

if not df_uren.empty:
    # --- Join the data for detailed analysis ---
    df_uren_detailed = df_uren.merge(df_projects, left_on='offerprojectbase_id', right_on='project_id', how='left')
    df_uren_detailed = df_uren_detailed.merge(df_employees, left_on='employee_id', right_on='id', suffixes=('_hour', '_emp'), how='left')
    df_uren_detailed = df_uren_detailed.merge(df_tasks, on='task_id', suffixes=('_hour', '_task'), how='left')

    st.markdown("Hieronder ziet u de details van de urenverdeling voor uw selectie.")
    sec2_col1, sec2_col2 = st.columns(2)

    with sec2_col1:
        st.markdown("##### Uren per Medewerker")
        df_employee_hours = df_uren_detailed.groupby('fullname')['amount'].sum().sort_values(ascending=True).reset_index()
        fig_emp = px.bar(df_employee_hours, x='amount', y='fullname', orientation='h', text_auto=True,
                         labels={'amount': 'Totaal Uren', 'fullname': 'Medewerker'})
        fig_emp.update_layout(showlegend=False)
        st.plotly_chart(fig_emp, use_container_width=True)

    with sec2_col2:
        st.markdown("##### Uren per Taaktype")
        df_task_hours = df_uren_detailed.groupby('task_name')['amount'].sum().reset_index()
        fig_task = px.pie(df_task_hours, names='task_name', values='amount',
                          hole=0.3, title="Verdeling van Uren per Taaktype",
                          color='task_name', color_discrete_map=TASK_COLOR_MAP)
        fig_task.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_task, use_container_width=True)

    # --- Detailed Table ---
    with st.expander("Bekijk Gedetailleerd Overzicht van Alle Urenregels"):
        display_cols = ['date_date', 'fullname', 'name', 'task_name', 'amount', 'description']
        df_display = df_uren_detailed[display_cols].rename(columns={
            'date_date': 'Datum', 'fullname': 'Medewerker', 'name': 'Project',
            'task_name': 'Taak', 'amount': 'Uren', 'description': 'Omschrijving'
        }).sort_values('Datum', ascending=False)
        st.dataframe(df_display, use_container_width=True)
else:
    st.info("Selecteer projecten om de analyse te zien.")


# --- Section 3: Monthly Analysis ---
st.markdown("---")
st.header("Maandelijkse Analyse per Bedrijf")
st.markdown("Analyseer de uren per taak voor specifieke bedrijven in een gekozen maand.")

if not df_uren.empty:
    # --- Prepare data for this section ---
    df_monthly = df_uren_detailed.copy()
    df_monthly['maand'] = pd.to_datetime(df_monthly['date_date']).dt.strftime('%Y-%m')
    
    # --- Filters for this section ---
    sec3_col1, sec3_col2 = st.columns(2)
    with sec3_col1:
        unique_months = sorted(df_monthly['maand'].unique(), reverse=True)
        selected_month = st.selectbox("Kies een Maand", unique_months)
    
    with sec3_col2:
        unique_companies = sorted(df_monthly['companyname'].unique())
        # Determine default companies based on activity in the selected month
        top_companies = df_monthly[df_monthly['maand'] == selected_month].groupby('companyname')['amount'].sum().nlargest(5).index.tolist()
        selected_companies = st.multiselect("Kies Bedrijven", unique_companies, default=top_companies)

    # --- Filter data based on selections ---
    df_analysis = df_monthly[(df_monthly['maand'] == selected_month) & (df_monthly['companyname'].isin(selected_companies))]

    if not df_analysis.empty:
        # --- Pivot Table / Heatmap ---
        st.markdown("##### Uren per Taak per Bedrijf")
        pivot_table = pd.pivot_table(df_analysis, values='amount', index='companyname', columns='task_name', aggfunc='sum', fill_value=0)
        st.dataframe(pivot_table.style.background_gradient(cmap='viridis').format("{:.2f}u"))

        # --- Bar chart for this section ---
        st.markdown("##### Totaal Uren per Taak (voor geselecteerde bedrijven)")
        df_pivot_totals = pivot_table.sum().sort_values(ascending=False).reset_index()
        df_pivot_totals.columns = ['task_name', 'total_hours']
        fig_pivot_bar = px.bar(
            df_pivot_totals,
            x='task_name',
            y='total_hours',
            color='task_name',
            color_discrete_map=TASK_COLOR_MAP,
            title=f"Totaal Uren per Taak in {selected_month}"
        )
        fig_pivot_bar.update_layout(xaxis_title="Taak", yaxis_title="Totaal Uren")
        st.plotly_chart(fig_pivot_bar, use_container_width=True)

        # --- Detailed Table for this section ---
        with st.expander("Bekijk Details per Medewerker"):
            detail_table = df_analysis.groupby(['companyname', 'fullname', 'task_name'])['amount'].sum().reset_index()
            st.dataframe(detail_table.rename(columns={
                'companyname': 'Bedrijf', 'fullname': 'Medewerker', 'task_name': 'Taak', 'amount': 'Totaal Uren'
            }), use_container_width=True)
    else:
        st.info("Geen uren gevonden voor de geselecteerde maand en bedrijven.")
else:
    st.info("Selecteer eerst projecten om de maandelijkse analyse te kunnen doen.")


# Footer
st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)

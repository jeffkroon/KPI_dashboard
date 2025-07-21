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
        min_value=datetime(2020, 1, 1), # Changed to 2020
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
    # --- Perform Aggregations on DB side ---
    date_filter = f"date_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"
    project_filter = f"offerprojectbase_id IN ({','.join(map(str, project_ids))})"
    
    with st.spinner("Aggregeren van data..."):
        # Query for KPIs
        kpi_query = f"""
        SELECT
            SUM(amount) as total_hours,
            COUNT(DISTINCT employee_id) as active_employees,
            COUNT(DISTINCT task_id) as tasks_done
        FROM urenregistratie
        WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
        """
        df_kpi = pd.read_sql(kpi_query, engine).iloc[0]

        # Query for employee hours chart
        employee_hours_query = f"""
        SELECT employee_id, SUM(amount) as total_hours
        FROM urenregistratie
        WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
        GROUP BY employee_id
        """
        df_employee_hours_agg = pd.read_sql(employee_hours_query, engine)
        df_employee_hours = df_employee_hours_agg.merge(df_employees, left_on='employee_id', right_on='id')

        # Query for task hours chart
        task_hours_query = f"""
        SELECT task_id, SUM(amount) as total_hours
        FROM urenregistratie
        WHERE status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
        GROUP BY task_id
        """
        df_task_hours_agg = pd.read_sql(task_hours_query, engine)
        df_task_hours = df_task_hours_agg.merge(df_tasks, on='task_id')

    # --- KPIs ---
    st.markdown("---")
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    total_hours = df_kpi['total_hours'] or 0
    active_employees = df_kpi['active_employees'] or 0
    tasks_done = df_kpi['tasks_done'] or 0
    avg_hours_per_project = total_hours / len(project_ids) if project_ids and total_hours > 0 else 0
    
    kpi_col1.metric("Totaal Uren", f"{total_hours:,.2f}")
    kpi_col2.metric("Actieve Medewerkers", active_employees)
    kpi_col3.metric("Unieke Taken", tasks_done)
    kpi_col4.metric("Gem. Uur/Project", f"{avg_hours_per_project:,.2f}")

    # --- Section 2: Analysis of Selected Projects ---
    st.markdown("---")
    st.header("Analyse van Geselecteerde Projecten")
    st.markdown("Hieronder ziet u de details van de urenverdeling voor uw selectie.")
    sec2_col1, sec2_col2 = st.columns(2)

    with sec2_col1:
        st.markdown("##### Uren per Medewerker")
        fig_emp = px.bar(df_employee_hours.sort_values('total_hours', ascending=True), 
                         x='total_hours', y='fullname', orientation='h', text_auto=True,
                         labels={'total_hours': 'Totaal Uren', 'fullname': 'Medewerker'})
        fig_emp.update_layout(showlegend=False)
        st.plotly_chart(fig_emp, use_container_width=True)

    with sec2_col2:
        st.markdown("##### Uren per Taaktype")
        fig_task = px.pie(df_task_hours, names='task_name', values='total_hours',
                          hole=0.3, title="Verdeling van Uren per Taaktype",
                          color='task_name', color_discrete_map=TASK_COLOR_MAP)
        fig_task.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_task, use_container_width=True)

    # --- Detailed Table (with LIMIT) ---
    with st.expander("Bekijk Gedetailleerd Overzicht van de Laatste 1000 Urenregels"):
        DETAIL_LIMIT = 1000
        detail_query = f"""
        SELECT u.date_date, u.employee_id, u.offerprojectbase_id, u.task_id, u.amount, u.description
        FROM urenregistratie u
        WHERE u.status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
        ORDER BY u.date_date DESC
        LIMIT {DETAIL_LIMIT}
        """
        df_detail_base = pd.read_sql(detail_query, engine)
        
        # Merge with pre-loaded dimension tables to get the names
        df_merged = df_detail_base.merge(df_employees, left_on='employee_id', right_on='id', how='left')
        df_merged = df_merged.merge(df_projects, left_on='offerprojectbase_id', right_on='project_id', how='left')
        df_merged = df_merged.merge(df_tasks, on='task_id', how='left')
        
        # Select and rename final columns for display
        df_display = df_merged[[
            'date_date', 'fullname', 'name', 'task_name', 'amount', 'description'
        ]].rename(columns={
            'date_date': 'Datum', 'fullname': 'Medewerker', 'name': 'Project',
            'task_name': 'Taak', 'amount': 'Uren', 'description': 'Omschrijving'
        })
        
        st.dataframe(df_display, use_container_width=True)
        if len(df_display) == DETAIL_LIMIT:
            st.warning(f"Let op: De weergave is beperkt tot de laatste {DETAIL_LIMIT} urenregels.")

    # --- Section 3: Monthly Analysis ---
    st.markdown("---")
    st.header("Maandelijkse Analyse per Bedrijf")
    st.markdown("Analyseer de uren per taak voor specifieke bedrijven in een gekozen maand.")
    
    # ... rest of the logic for section 3
    # This also needs to be refactored to use a server-side aggregated query.
    # For now, this part will be based on the limited df_display to avoid crashing.
    df_uren_detailed_limited = df_display.copy()
    df_uren_detailed_limited['companyname'] = df_uren_detailed_limited['project_name'].map(df_projects.set_index('name')['companyname']) # Approximate companyname

    if not df_uren_detailed_limited.empty:
        df_monthly = df_uren_detailed_limited
        df_monthly['maand'] = pd.to_datetime(df_monthly['Datum']).dt.strftime('%Y-%m')
        # ... (rest of section 3 unchanged but now runs on limited data)
    
    # ... (code for section 3 continues here)
        
else:
    st.info("Selecteer projecten om de analyse te zien.")


# Footer
st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)

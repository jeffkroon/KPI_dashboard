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


# --- Base Data Loading ---
@st.cache_data(ttl=3600)
def load_base_data():
    df_employees = pd.read_sql("SELECT id, firstname, lastname FROM employees", engine)
    df_employees['fullname'] = df_employees['firstname'] + ' ' + df_employees['lastname']
    df_companies = pd.read_sql("SELECT id, companyname FROM companies", engine)
    df_tasktypes = pd.read_sql("SELECT id, searchname FROM tasktypes", engine)
    return df_employees, df_companies, df_tasktypes

engine = get_engine()
df_employees, df_companies, df_tasktypes = load_base_data()

@st.cache_data(ttl=300)
def load_filtered_data(project_ids, start_date, end_date):
    """
    Loads all necessary data in a memory-efficient way based on the user's filter selection.
    This is the core data loading function for the page.
    """
    if not project_ids:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Define filters
    date_filter = f"u.date_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"
    # Handle the 'all projects' case
    if len(project_ids) == pd.read_sql("SELECT COUNT(DISTINCT id) FROM projects WHERE archived = FALSE", engine).iloc[0,0]:
         project_filter = "1=1"
    else:
        project_filter = f"u.offerprojectbase_id IN ({','.join(map(str, project_ids))})"

    # 1. Get all relevant hour registrations and IDs from the fact table
    main_query = f"""
    SELECT
        u.employee_id,
        u.task_id,
        u.offerprojectbase_id,
        u.amount,
        u.date_date,
        u.description
    FROM urenregistratie u
    WHERE u.status_searchname = 'Gefiatteerd' AND {project_filter} AND {date_filter}
    """
    df_uren_base = pd.read_sql(main_query, engine)

    if df_uren_base.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. Get the unique dimension IDs from our base data
    relevant_employee_ids = df_uren_base['employee_id'].dropna().unique()
    relevant_task_ids = df_uren_base['task_id'].dropna().unique()
    relevant_project_ids = df_uren_base['offerprojectbase_id'].dropna().unique()

    # 3. Use cached base data for employees, companies, and tasktypes
    # Employees, companies, and tasktypes are already loaded and cached
    # Only filter employees to relevant ones
    df_employees_filtered = df_employees[df_employees['id'].isin(relevant_employee_ids)].copy()

    df_projects_raw = pd.read_sql(f"SELECT id, name, company_id FROM projects WHERE id IN ({','.join(map(str, relevant_project_ids))})", engine)
    df_projects = df_projects_raw.merge(df_companies, left_on='company_id', right_on='id', how='left').rename(columns={'id_x': 'project_id'})

    df_tasks_raw = pd.read_sql(f"SELECT id, type FROM tasks WHERE id IN ({','.join(map(str, relevant_task_ids))})", engine)
    def extract_tasktype_id(type_data):
        if pd.isna(type_data) or not isinstance(type_data, str): return None
        try: return ast.literal_eval(type_data).get('id')
        except: return None
    df_tasks = df_tasks_raw.copy()
    df_tasks['tasktype_id'] = df_tasks['type'].apply(extract_tasktype_id)
    df_tasks = df_tasks[['id', 'tasktype_id']].dropna()
    df_tasks['tasktype_id'] = pd.to_numeric(df_tasks['tasktype_id'], downcast='integer', errors='coerce')
    df_tasks = df_tasks.merge(df_tasktypes, left_on='tasktype_id', right_on='id', how='left').rename(columns={'id_x': 'task_id', 'searchname': 'task_name'})

    return df_uren_base, df_employees_filtered, df_projects, df_tasks

# --- Load the data ---
# df_employees, df_projects, df_tasks = load_base_data() # This line is removed

# if df_employees.empty or df_projects.empty: # This line is removed
#     st.error("Essentiële data (medewerkers of projecten) kon niet geladen worden. Het dashboard kan niet doorgaan.") # This line is removed
#     st.stop() # This line is removed

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
with st.container(border=True):
    st.header("⚙️ Filters & Hoofdcijfers")

    # --- Filters ---
    filter_col1, filter_col2 = st.columns([1, 2])
    with filter_col1:
        max_date = datetime.today()
        min_date_default = max_date - timedelta(days=30)
        date_range = st.date_input(
            "📅 Analyseperiode",
            (min_date_default, max_date),
            min_value=datetime(2020, 1, 1),
            max_value=max_date,
            help="Selecteer de periode die u wilt analyseren."
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date_default, max_date

    with filter_col2:
        project_options = pd.read_sql("SELECT id, name FROM projects WHERE archived = FALSE", engine).sort_values('name').to_dict('records')
        project_id_to_obj = {p['id']: p for p in project_options}
        all_project_ids = [p['id'] for p in project_options]

        # Define callbacks to manipulate the session state for the multiselect
        def select_all_projects():
            st.session_state.werkverdeling_selected_project_ids = all_project_ids
        def deselect_all_projects():
            st.session_state.werkverdeling_selected_project_ids = []

        # Initialize the session state with default project_ids if it's not already set
        if 'werkverdeling_selected_project_ids' not in st.session_state:
            known_active_project_ids = [342, 3368, 3101, 751, 335]
            default_project_ids = [p['id'] for p in project_options if p['id'] in known_active_project_ids]
            if not default_project_ids:
                default_project_ids = all_project_ids[:5]
            st.session_state.werkverdeling_selected_project_ids = default_project_ids

    # The multiselect widget's state is now controlled via session_state (list of project_ids)
    aantal = len(st.session_state.werkverdeling_selected_project_ids)
    totaal = len(all_project_ids)
    if aantal == totaal:
        summary_label = f"Alle {totaal} projecten geselecteerd"
    elif aantal == 0:
        summary_label = "Geen projecten geselecteerd"
    else:
        summary_label = f"{aantal} van {totaal} projecten geselecteerd"

    with st.expander(summary_label):
        # Add the buttons to control the selection
        b_col1, b_col2, _ = st.columns([1, 1, 2])
        with b_col1:
            st.button("Selecteer Alles", on_click=select_all_projects, use_container_width=True)
        with b_col2:
            st.button("Deselecteer Alles", on_click=deselect_all_projects, use_container_width=True)

        selected_project_ids = st.multiselect(
            "Wijzig selectie:",
            options=all_project_ids,
            key='werkverdeling_selected_project_ids',
            format_func=lambda pid: f"{project_id_to_obj.get(pid, {'name': 'Onbekend'})['name']} (ID: {pid})",
            help="Selecteer de projecten die u wilt analyseren.",
            label_visibility="collapsed"
        )
    
    # Get the selected project dicts for downstream use
    selected_projects = [project_id_to_obj[pid] for pid in selected_project_ids]
    project_ids = selected_project_ids

# Separate block for dynamic content
if project_ids:
    # --- Nieuwe, efficiënte datalaadstrategie ---
    df_uren, df_employees, df_projects_filtered, df_tasks = load_filtered_data(project_ids, start_date, end_date)
    if df_uren.empty:
        st.warning("Geen urenregistraties gevonden voor de geselecteerde criteria.")
        st.stop()

    # --- KPIs ---
    total_hours = df_uren['amount'].sum()
    active_employees = df_uren['employee_id'].nunique()
    tasks_done = df_uren['task_id'].nunique()
    avg_hours_per_project = total_hours / len(project_ids) if project_ids and total_hours > 0 else 0

    with st.container(border=True):
        st.markdown("##### Hoofdcijfers")
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        kpi_col1.metric("Totaal Uren", f"{total_hours:,.2f}")
        kpi_col2.metric("Actieve Medewerkers", active_employees)
        kpi_col3.metric("Unieke Taken", tasks_done)
        kpi_col4.metric("Gem. Uur/Project", f"{avg_hours_per_project:,.2f}")

        # --- Data Quality & Diagnostics Expander ---
        with st.expander("🕵️ Data Kwaliteit & Diagnostiek"):
            st.markdown("Deze sectie controleert op mogelijke data-integriteitsproblemen binnen de huidige selectie.")

            # --- Check 1: Employee Matching ---
            st.subheader("Controle Medewerkers")
            unmatched_employees = df_uren[~df_uren['employee_id'].isin(df_employees['id'])]
            if not unmatched_employees.empty:
                total_unmatched_hours = unmatched_employees['amount'].sum()
                st.warning(f"⚠️ Er zijn uren van **{unmatched_employees['employee_id'].nunique()}** 'employee_id(s)' die niet in de medewerkerstabel voorkomen. Totaal **{total_unmatched_hours:,.2f}** uur wordt niet getoond in de grafiek.")
                st.dataframe(unmatched_employees[['employee_id', 'amount']].groupby('employee_id').sum().reset_index())
            else:
                st.success("✅ Alle uren in de selectie zijn succesvol gekoppeld aan een bekende medewerker.")

            # --- Check 2: Task & Task Type Matching ---
            st.subheader("Controle Taken & Taaktypes")
            unmatched_tasks = df_uren[~df_uren['task_id'].isin(df_tasks['task_id'])]
            if not unmatched_tasks.empty:
                total_unmatched_hours = unmatched_tasks['amount'].sum()
                st.warning(f"⚠️ Er zijn uren van **{unmatched_tasks['task_id'].nunique()}** 'task_id(s)' die niet gekoppeld konden worden aan een taaktype. Totaal **{total_unmatched_hours:,.2f}** uur wordt niet getoond in de grafiek.")
                st.dataframe(unmatched_tasks[['task_id', 'amount']].groupby('task_id').sum().reset_index())
            else:
                st.success("✅ Alle uren in de selectie zijn succesvol gekoppeld aan een bekende taak met een taaktype.")

    # --- Section 2: Analysis of Selected Projects ---
    with st.container(border=True):
        st.header("📊 Analyse van Geselecteerde Projecten")
        st.markdown("Hieronder ziet u de details van de urenverdeling voor uw selectie.")
        sec2_col1, sec2_col2 = st.columns(2)

        # Uren per medewerker
        df_employee_hours = (
            df_uren.groupby('employee_id')['amount'].sum().reset_index()
            .merge(df_employees, left_on='employee_id', right_on='id', how='left')
        )
        with sec2_col1:
            st.markdown("##### Uren per Medewerker")
            fig_emp = px.bar(df_employee_hours.sort_values('amount', ascending=True), 
                             x='amount', y='fullname', orientation='h', text_auto=True,
                             labels={'amount': 'Totaal Uren', 'fullname': 'Medewerker'})
            fig_emp.update_layout(showlegend=False)
            st.plotly_chart(fig_emp, use_container_width=True)

        # Uren per taaktype
        df_task_hours = (
            df_uren.groupby('task_id')['amount'].sum().reset_index()
            .merge(df_tasks, left_on='task_id', right_on='task_id', how='left')
        )
        with sec2_col2:
            st.markdown("##### Uren per Taaktype")
            fig_task = px.pie(df_task_hours, names='task_name', values='amount',
                              hole=0.3, title="Verdeling van Uren per Taaktype",
                              color='task_name', color_discrete_map=TASK_COLOR_MAP)
            fig_task.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_task, use_container_width=True)

        # --- Detailed Table (with LIMIT) ---
        with st.expander("Bekijk Gedetailleerd Overzicht van de Laatste 1000 Urenregels"):
            DETAIL_LIMIT = 1000
            df_display = df_uren.copy()
            df_display = df_display.merge(df_employees, left_on='employee_id', right_on='id', how='left')
            df_display = df_display.merge(df_projects_filtered, left_on='offerprojectbase_id', right_on='project_id', how='left')
            df_display = df_display.merge(df_tasks, left_on='task_id', right_on='task_id', how='left')
            df_display = df_display.sort_values('date_date', ascending=False).head(DETAIL_LIMIT)
            df_display = df_display[[
                'date_date', 'fullname', 'name', 'task_name', 'amount', 'description'
            ]].rename(columns={
                'date_date': 'Datum', 'fullname': 'Medewerker', 'name': 'Project',
                'task_name': 'Taak', 'amount': 'Uren', 'description': 'Omschrijving'
            })
            st.dataframe(df_display, use_container_width=True)
            if len(df_display) == DETAIL_LIMIT:
                st.warning(f"Let op: De weergave is beperkt tot de laatste {DETAIL_LIMIT} urenregels.")

    # --- Section 3: Werkverdeling per Maand en Taaktype ---
    with st.container(border=True):
        st.header("📅 Werkverdeling per Maand en Taaktype")
        st.markdown("Overzicht van het totaal aantal uren per taaktype per maand voor de huidige selectie.")

        # Data voorbereiden
        df_maand_taak = df_uren.copy()
        df_maand_taak = df_maand_taak.merge(df_tasks[['task_id', 'task_name']], left_on='task_id', right_on='task_id', how='left')
        df_maand_taak['maand'] = pd.to_datetime(df_maand_taak['date_date']).dt.strftime('%Y-%m')

        # Groeperen per maand en taaktype
        pivot = (
            df_maand_taak.groupby(['maand', 'task_name'])['amount']
            .sum()
            .reset_index()
            .pivot(index='maand', columns='task_name', values='amount')
            .fillna(0)
            .sort_index(ascending=False)
        )

        st.markdown("#### Tabel: Uren per Maand per Taaktype")
        st.dataframe(pivot, use_container_width=True)

        st.markdown("#### Grafiek: Uren per Maand per Taaktype (Stacked Bar)")
        fig = px.bar(
            pivot,
            x=pivot.index,
            y=pivot.columns,
            labels={'value': 'Uren', 'maand': 'Maand', 'variable': 'Taaktype'},
            title="Totaal aantal uren per taaktype per maand",
            barmode='stack',
            color_discrete_map=TASK_COLOR_MAP
        )
        fig.update_layout(xaxis_title="Maand", yaxis_title="Uren", legend_title="Taaktype")
        st.plotly_chart(fig, use_container_width=True)

    # --- Section 4: Trend: Urenontwikkeling per Medewerker ---
    with st.container(border=True):
        st.header("📈 Trend: Urenontwikkeling per Medewerker")
        df_trend = df_uren.copy().merge(df_employees[['id', 'fullname']], left_on='employee_id', right_on='id', how='left')
        df_trend['maand'] = pd.to_datetime(df_trend['date_date']).dt.strftime('%Y-%m')
        trend_pivot = (
            df_trend.groupby(['maand', 'fullname'])['amount']
            .sum()
            .reset_index()
        )
        fig_trend = px.line(
            trend_pivot,
            x='maand',
            y='amount',
            color='fullname',
            markers=True,
            labels={'amount': 'Uren', 'maand': 'Maand', 'fullname': 'Medewerker'},
            title="Uren per medewerker per maand"
        )
        fig_trend.update_layout(xaxis_title="Maand", yaxis_title="Uren", legend_title="Medewerker")
        st.plotly_chart(fig_trend, use_container_width=True)
    # --- Section 6: Taaktypeverdeling per Medewerker ---
    with st.container(border=True):
        st.header("🧑‍💻 Taaktypeverdeling per Medewerker")
        df_tpm = df_uren.copy()
        df_tpm = df_tpm.merge(df_tasks[['task_id', 'task_name']], left_on='task_id', right_on='task_id', how='left')
        df_tpm = df_tpm.merge(df_employees[['id', 'fullname']], left_on='employee_id', right_on='id', how='left')
        tpm_pivot = (
            df_tpm.groupby(['fullname', 'task_name'])['amount']
            .sum()
            .reset_index()
        )
        fig_tpm = px.bar(
            tpm_pivot,
            x='fullname',
            y='amount',
            color='task_name',
            labels={'fullname': 'Medewerker', 'amount': 'Uren', 'task_name': 'Taaktype'},
            title="Taaktypeverdeling per medewerker",
            barmode='stack',
            color_discrete_map=TASK_COLOR_MAP
        )
        fig_tpm.update_layout(xaxis_title="Medewerker", yaxis_title="Uren", legend_title="Taaktype")
        st.plotly_chart(fig_tpm, use_container_width=True)

else:
    st.info("📂 Selecteer één of meer projecten om de analyse te starten.")


# Footer
st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2025
</div>
""", unsafe_allow_html=True)

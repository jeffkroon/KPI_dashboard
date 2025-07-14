import streamlit as st
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import plotly.express as px
import numpy as np
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS

st.set_page_config(
    page_title="Werkverdeling",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

require_login()
require_email_whitelist(ALLOWED_EMAILS)

if "access_token" in st.session_state:
    st.sidebar.write(f"Ingelogd als: {st.session_state.get('user_email', '')}")
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()


st.logo("images/dunion-logo-def_donker-06.png")

# Data & omgeving setup
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    st.error("POSTGRES_URL is not set in the environment.")
    st.stop()
engine = create_engine(POSTGRES_URL)

def load_data(table_name, start_date=None, end_date=None, limit=5000):
    if table_name == "urenregistratie":
        query = f"SELECT * FROM {table_name} WHERE status_searchname = 'Gefiatteerd'"
        if start_date and end_date:
            query += f" AND date_date::timestamp BETWEEN '{start_date}' AND '{end_date}'"
        query += f" LIMIT {limit}"
    elif table_name == "employees":
        query = f"SELECT * FROM {table_name};"  # GEEN LIMIT voor employees
    else:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
    return pd.read_sql(query, con=engine)

# Datasets laden
df_employees = load_data("employees")
df_employees['fullname'] = df_employees['firstname'] + " " + df_employees['lastname']
df_projects = load_data("projects")
df_companies = load_data("companies")

# Filter niet-gearchiveerde projecten
df_projects = df_projects[df_projects["archived"] != True]

# Zorg dat totalexclvat numeriek is voor projecten
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")

# Projecten met klantnaam mergen
df_projects = df_projects.merge(
    df_companies[['id', 'companyname']], left_on='company_id', right_on='id', how='left', suffixes=('', '_company')
)

# Eén datumrange-selector bovenaan
min_date = pd.to_datetime("2023-01-01")
max_date = pd.to_datetime("today")
date_range = st.date_input("Selecteer datumrange urenregistratie", [min_date, max_date])
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# Data ophalen met filters
# Voor urenregistratie:
df_uren = load_data("urenregistratie", start_date, end_date)
# Voor andere tabellen:
df_employees = load_data("employees")

# Pagina titel
st.title("📋 Project Overzicht met Medewerker Uren")

# --- Filters bovenaan ---
# Project opties en standaard selectie
project_options = df_projects['name'].unique().tolist()
# Alleen eerste 10 projecten standaard geselecteerd
default_projects = project_options[:10]

# Checkbox voor 'Selecteer alles'
select_all_projects = st.checkbox("Selecteer alle projecten", value=False)
if select_all_projects:
    selected_projects = project_options
else:
    selected_projects = st.multiselect(
        "Selecteer één of meerdere projecten",
        options=project_options,
        default=default_projects,
        help="Gebruik het zoekveld om projecten te vinden"
    )
# Filter uren op geselecteerde projecten (via offerprojectbase_id)
project_ids = pd.Series(df_projects[df_projects['name'].isin(selected_projects)]['id']).to_list()
df_uren_filtered = df_uren[df_uren['offerprojectbase_id'].isin(project_ids)].copy()

# KPI-berekeningen
aantal_projecten = len(selected_projects)
totale_omzet = df_projects[df_projects['name'].isin(selected_projects)]['totalexclvat'].sum()

# Medewerkers betrokken bij geselecteerde projecten
medewerkers_ids = pd.Series(df_uren_filtered['employee_id']).unique().tolist()
aantal_medewerkers = len(medewerkers_ids)

# Totale uren geschreven aan geselecteerde projecten
totale_uren = df_uren_filtered['amount'].sum()

# KPI's tonen
col1, col2, col3, col4 = st.columns(4)
col1.metric("Aantal geselecteerde projecten", aantal_projecten)
col2.metric("Totale projectomzet (excl. btw)", f"€ {totale_omzet:,.2f}")
col3.metric("Aantal medewerkers betrokken", aantal_medewerkers)
col4.metric("Totale uren geschreven", f"{totale_uren:.2f}")

# Medewerker details tabel
if aantal_medewerkers > 0:
    df_medewerkers_filtered = df_employees[df_employees['id'].isin(medewerkers_ids)].copy()
    
    # Uren per medewerker voor de geselecteerde projecten
    uren_per_medewerker = df_uren_filtered.groupby('employee_id')['amount'].sum().to_dict()
    # Totale uren per medewerker over alle projecten (voor percentage)
    totale_uren_per_medewerker = df_uren.groupby('employee_id')['amount'].sum().to_dict()
    ids = pd.Series(df_medewerkers_filtered['id'])
    df_medewerkers_filtered['Uren aan geselecteerde projecten'] = ids.apply(lambda x: uren_per_medewerker.get(x, 0))
    df_medewerkers_filtered['Totale uren'] = ids.apply(lambda x: totale_uren_per_medewerker.get(x, 0))
    # Zorg dat Percentage uren een Series is voor replace/fillna
    perc_uren = pd.Series(df_medewerkers_filtered['Uren aan geselecteerde projecten']) / pd.Series(df_medewerkers_filtered['Totale uren']).replace(0, np.nan)
    perc_uren = perc_uren.astype(float).fillna(0) * 100
    df_medewerkers_filtered['Percentage uren'] = perc_uren
    df_medewerkers_filtered['fullname'] = df_medewerkers_filtered['firstname'] + " " + df_medewerkers_filtered['lastname']
    
    # Optioneel: functie of uurtarief tonen als kolom als beschikbaar
    if 'function' in df_medewerkers_filtered.columns and isinstance(df_medewerkers_filtered, pd.DataFrame):
        df_medewerkers_filtered = df_medewerkers_filtered.rename(columns={'function': 'Functie'})
    else:
        df_medewerkers_filtered['Functie'] = "Onbekend"
    
    df_display = df_medewerkers_filtered[['fullname', 'Uren aan geselecteerde projecten', 'Percentage uren', 'Functie']].copy()
    if not isinstance(df_display, pd.DataFrame):
        df_display = pd.DataFrame(df_display)
    df_display = df_display.sort_values(by='Uren aan geselecteerde projecten', ascending=False).copy()
    
    st.subheader("👷 Medewerkers die aan geselecteerde projecten werken")
    st.dataframe(df_display.style.format({
        'Uren aan geselecteerde projecten': "{:,.2f}",
        'Percentage uren': "{:.1f} %"
    }))
else:
    st.info("Geen medewerkers gevonden die uren hebben geschreven aan de geselecteerde projecten.")

# Optionele filter op medewerkers binnen geselecteerde projecten
st.subheader("Filter medewerkers binnen geselecteerde projecten")
medewerkers = pd.Series(df_employees['firstname']).unique().tolist()
selected_medewerkers = st.multiselect("Selecteer medewerker(s)", medewerkers)
medewerker_ids_filter = pd.Series(df_employees[df_employees['firstname'].isin(selected_medewerkers)]['id']).to_list()

# Visualisaties
st.subheader("📊 Visualisaties")

if aantal_medewerkers > 0:
    df_vis = df_uren_filtered.copy()
    if selected_medewerkers:
        medewerker_ids_filter = list(medewerker_ids_filter)
        df_vis = df_vis[pd.Series(df_vis['employee_id']).isin(medewerker_ids_filter)]
    
    # Uren per medewerker bar chart
    if isinstance(df_vis, pd.DataFrame):
        uren_per_medewerker_vis = df_vis.groupby('employee_id')['amount'].sum()
        df_med_uren = df_employees.set_index('id').loc[uren_per_medewerker_vis.index]
        df_med_uren['Uren'] = uren_per_medewerker_vis.values
        df_med_uren['fullname'] = df_med_uren['firstname'] + " " + df_med_uren['lastname']
        
        fig1 = px.bar(df_med_uren.sort_values('Uren', ascending=True), x='Uren', y='fullname', orientation='h',
                      title='Uren per medewerker (filter toepasbaar)', labels={'Uren': 'Uren', 'fullname': 'Medewerker'},
                      color='fullname', color_discrete_sequence=px.colors.qualitative.Plotly, text_auto=True)
        fig1.update_layout(showlegend=False, template="plotly_white")
        st.plotly_chart(fig1, use_container_width=True)
    
    # Uren per taak bar chart
    if isinstance(df_vis, pd.DataFrame) and 'task_searchname' in df_uren_filtered.columns:
        uren_per_taak = df_vis.groupby('task_searchname')['amount'].sum()
        df_taak = uren_per_taak.reset_index().sort_values('amount', ascending=False).head(10)
        fig2 = px.bar(df_taak, x='task_searchname', y='amount',
                      title='Top 10 taken per uren', labels={'task_searchname': 'Taak', 'amount': 'Uren'},
                      color='task_searchname', color_discrete_sequence=px.colors.qualitative.Plotly, text_auto=True)
        fig2.update_layout(showlegend=False, xaxis_tickangle=-45, template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("Geen data beschikbaar voor visualisaties.")

st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)
    
    

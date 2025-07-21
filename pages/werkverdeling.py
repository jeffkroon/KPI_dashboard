import streamlit as st
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import plotly.express as px
import numpy as np
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS
from utils.data_loaders import load_data, load_data_df
from datetime import datetime

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

# Datasets laden
df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
if not isinstance(df_employees, pd.DataFrame):
    df_employees = pd.concat(list(df_employees), ignore_index=True)
df_employees['fullname'] = df_employees['firstname'] + " " + df_employees['lastname']

df_projects = load_data_df("projects", columns=["id", "name", "company_id", "archived", "totalexclvat", "phase_searchname"])
if not isinstance(df_projects, pd.DataFrame):
    df_projects = pd.concat(list(df_projects), ignore_index=True)

df_companies = load_data_df("companies", columns=["id", "companyname"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)
    
df_tasks = load_data_df("tasks", columns=["id", "type"])
if not isinstance(df_tasks, pd.DataFrame):
    df_tasks = pd.concat(list(df_tasks), ignore_index=True)
df_tasks['tasktype_id'] = df_tasks['type'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

df_tasktypes = load_data_df("tasktypes", columns=["id", "searchname"])
if not isinstance(df_tasktypes, pd.DataFrame):
    df_tasktypes = pd.concat(list(df_tasktypes), ignore_index=True)

# Filter niet-gearchiveerde projecten
df_projects = df_projects[(df_projects["archived"] == False) & (df_projects["phase_searchname"].isin(["Voorbereiding", "Uitvoering"]))]
print(f"[DEBUG] Aantal projecten Voorbereiding/Uitvoering (niet-gearchiveerd): {len(df_projects)}")

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
df_uren = load_data_df("urenregistratie", columns=["id", "offerprojectbase_id", "employee_id", "task_id", "amount", "task_searchname", "date_date", "status_searchname"], where=f"status_searchname = 'Gefiatteerd' AND date_date::timestamp BETWEEN '{start_date}' AND '{end_date}'")
if not isinstance(df_uren, pd.DataFrame):
    df_uren = pd.concat(list(df_uren), ignore_index=True)

# Koppel uren aan tasks en tasktypes
df_uren = df_uren.merge(df_tasks[['id', 'tasktype_id']], left_on='task_id', right_on='id', how='left')

# Fix data type mismatch voor tasktype_id
if 'tasktype_id' in df_uren.columns:
    # Converteer tasktype_id naar numeriek, met errors='coerce' voor niet-numerieke waarden
    df_uren['tasktype_id'] = pd.to_numeric(df_uren['tasktype_id'], errors='coerce')

df_uren = df_uren.merge(df_tasktypes.rename(columns={'id': 'tasktype_id', 'searchname': 'tasktype_general_name'}), on='tasktype_id', how='left')

# Koppel uren aan employees
df_uren = df_uren.merge(df_employees[['id', 'fullname']], left_on='employee_id', right_on='id', how='left', suffixes=('', '_employee'))

# Voeg maandkolom toe
df_uren['maand'] = pd.to_datetime(df_uren['date_date']).dt.to_period('M').astype(str)

# Voor andere tabellen:
# df_employees = load_data("employees") # This line is removed as df_employees is now loaded globally

# Pagina titel
st.title("📋 Opdracht Overzicht met Medewerker Uren")

# --- Filters bovenaan ---
# Project opties en standaard selectie
project_options = df_projects[['id', 'name']].drop_duplicates().to_dict('records')
# Alleen eerste 10 projecten standaard geselecteerd
default_projects = project_options[:10]

# Checkbox voor 'Selecteer alles'
select_all_projects = st.checkbox("Selecteer alle opdrachten", value=False)
if select_all_projects:
    selected_projects = project_options
else:
    selected_projects = st.multiselect(
        "Selecteer één of meerdere opdrachten",
        options=project_options,
        default=default_projects,
        format_func=lambda x: f"{x['name']} (ID: {x['id']})",
        help="Gebruik het zoekveld om opdrachten te vinden"
    )
# Filter uren op geselecteerde projecten (via offerprojectbase_id)
project_ids = [p['id'] for p in selected_projects]
df_uren_filtered = df_uren[df_uren['offerprojectbase_id'].isin(project_ids)].copy()

# KPI-berekeningen
# Aantal geselecteerde projecten op basis van unieke IDs
aantal_projecten = len(project_ids)
totale_omzet = df_projects[df_projects['id'].isin(project_ids)]['totalexclvat'].sum()

# Medewerkers betrokken bij geselecteerde projecten
medewerkers_ids = pd.Series(df_uren_filtered['employee_id']).unique().tolist()
aantal_medewerkers = len(medewerkers_ids)

# Totale uren geschreven aan geselecteerde projecten
totale_uren = df_uren_filtered['amount'].sum()

# KPI's tonen
col1, col2, col3, col4 = st.columns(4)
col1.metric("Aantal geselecteerde opdrachten", aantal_projecten)
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
    df_medewerkers_filtered['Uren aan geselecteerde opdrachten'] = ids.apply(lambda x: uren_per_medewerker.get(x, 0))
    df_medewerkers_filtered['Totale uren'] = ids.apply(lambda x: totale_uren_per_medewerker.get(x, 0))
    # Zorg dat Percentage uren een Series is voor replace/fillna
    perc_uren = pd.Series(df_medewerkers_filtered['Uren aan geselecteerde opdrachten']) / pd.Series(df_medewerkers_filtered['Totale uren']).replace(0, np.nan)
    perc_uren = perc_uren.astype(float).fillna(0) * 100
    df_medewerkers_filtered['Percentage uren'] = perc_uren
    df_medewerkers_filtered['fullname'] = df_medewerkers_filtered['firstname'] + " " + df_medewerkers_filtered['lastname']

    # Optioneel: functie of uurtarief tonen als kolom als beschikbaar
    if 'function' in df_medewerkers_filtered.columns and isinstance(df_medewerkers_filtered, pd.DataFrame):
        df_medewerkers_filtered = df_medewerkers_filtered.rename(columns={'function': 'Functie'})
    else:
        df_medewerkers_filtered['Functie'] = "Onbekend"

    df_display = df_medewerkers_filtered[['fullname', 'Uren aan geselecteerde opdrachten', 'Percentage uren', 'Functie']].copy()
    if not isinstance(df_display, pd.DataFrame):
        df_display = pd.DataFrame(df_display)
    df_display = df_display.sort_values(by='Uren aan geselecteerde opdrachten', ascending=False).copy()

    st.subheader("👷 Medewerkers die aan geselecteerde opdrachten werken")
    st.dataframe(df_display.style.format({
        'Uren aan geselecteerde opdrachten': "{:,.2f}",
        'Percentage uren': "{:.1f} %"
    }))
else:
    st.info("Geen medewerkers gevonden die uren hebben geschreven aan de geselecteerde opdrachten.")

# Optionele filter op medewerkers binnen geselecteerde projecten
st.subheader("Filter medewerkers binnen geselecteerde opdrachten")
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
    if isinstance(df_vis, pd.DataFrame) and 'tasktype_general_name' in df_vis.columns:
        uren_per_taak = df_vis.groupby('tasktype_general_name')['amount'].sum()
        df_taak = uren_per_taak.reset_index().sort_values('amount', ascending=False).head(10)
        fig2 = px.bar(df_taak, x='tasktype_general_name', y='amount',
                      title='Top 10 taken per uren', labels={'tasktype_general_name': 'Taaktype', 'amount': 'Uren'},
                      color='tasktype_general_name', color_discrete_sequence=px.colors.qualitative.Plotly, text_auto=True)
        fig2.update_layout(showlegend=False, xaxis_tickangle=-45, template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("Geen data beschikbaar voor visualisaties.")

# === Uren per taaktype per maand (gefilterd op projecten) ===
st.subheader("📊 Uren per taaktype per maand (gefilterd op projecten)")

# Voeg maandkolom toe
if not df_uren_filtered.empty:
    df_uren_filtered['maand'] = pd.to_datetime(df_uren_filtered['date_date']).dt.to_period('M')
    uren_per_maand_taak_filtered = df_uren_filtered.groupby(['maand', 'tasktype_general_name'])['amount'].sum().reset_index()
    uren_per_maand_taak_filtered['maand'] = uren_per_maand_taak_filtered['maand'].astype(str)
    uren_per_maand_taak_filtered = uren_per_maand_taak_filtered.sort_values(['maand', 'tasktype_general_name'])

    # Toon als tabel
    st.dataframe(
        uren_per_maand_taak_filtered.rename(columns={
            'maand': 'Maand',
            'tasktype_general_name': 'Taaktype',
            'amount': 'Totaal uren'
        }),
        use_container_width=True
    )

    # Stacked bar chart
    fig_filtered = px.bar(
        uren_per_maand_taak_filtered,
        x='maand',
        y='amount',
        color='tasktype_general_name',
        title='Uren per taaktype per maand (gefilterd op projecten)',
        labels={'amount': 'Uren', 'maand': 'Maand', 'tasktype_general_name': 'Taaktype'},
        text_auto=True
    )
    fig_filtered.update_layout(
        barmode='stack',
        xaxis_title='Maand',
        yaxis_title='Totaal uren',
        legend_title='Taaktype',
        template='plotly_white',
        margin=dict(l=40, r=40, t=60, b=40)
    )
    st.plotly_chart(fig_filtered, use_container_width=True, key="filtered_tasktype_chart")
else:
    st.info("Geen uren gevonden voor de geselecteerde periode en projecten.")

# --- Multiselect voor medewerkers ---
alle_medewerkers = df_uren['fullname'].dropna().unique().tolist()

# Checkbox voor 'Selecteer alles' voor medewerkers
select_all_medewerkers = st.checkbox("Selecteer alle medewerkers", value=False, key="select_all_medewerkers")
if select_all_medewerkers:
    geselecteerde_medewerkers = alle_medewerkers
else:
    geselecteerde_medewerkers = st.multiselect(
        "Selecteer medewerker(s) voor detailoverzicht",
        options=alle_medewerkers,
        default=alle_medewerkers[:3],
        help="Selecteer één of meerdere medewerkers om hun urenverdeling te zien"
    )

# === Overzicht 1: Uren per maand per algemeen taaktype (alle uren) ===
st.subheader("📊 Uren per maand per algemeen taaktype (alle uren)")
uren_per_maand_taak_general = df_uren.groupby(['maand', 'tasktype_general_name'])['amount'].sum().reset_index()
uren_per_maand_taak_general = uren_per_maand_taak_general.sort_values(['maand', 'tasktype_general_name'])
st.dataframe(
    uren_per_maand_taak_general.rename(columns={
        'maand': 'Maand',
        'tasktype_general_name': 'Taaktype',
        'amount': 'Totaal uren'
    }),
    use_container_width=True
)
fig1 = px.bar(
    uren_per_maand_taak_general,
    x='maand',
    y='amount',
    color='tasktype_general_name',
    title='Uren per taaktype per maand (alle uren)',
    labels={'amount': 'Uren', 'maand': 'Maand', 'tasktype_general_name': 'Taaktype'},
    text_auto=True
)
fig1.update_layout(barmode='stack', xaxis_title='Maand', yaxis_title='Totaal uren', legend_title='Taaktype', template='plotly_white', margin=dict(l=40, r=40, t=60, b=40))
st.plotly_chart(fig1, use_container_width=True, key="general_tasktype_chart")

# === Overzicht 2: Uren per maand per taaktype per medewerker ===
st.subheader("📊 Uren per maand per taaktype per medewerker(s)")
if geselecteerde_medewerkers:
    df_uren_sel = df_uren[df_uren['fullname'].isin(geselecteerde_medewerkers)].copy()
    uren_per_maand_taak_med = df_uren_sel.groupby(['maand', 'fullname', 'tasktype_general_name'])['amount'].sum().reset_index()
    uren_per_maand_taak_med = uren_per_maand_taak_med.sort_values(['maand', 'fullname', 'tasktype_general_name'])
    st.dataframe(
        uren_per_maand_taak_med.rename(columns={
            'maand': 'Maand',
            'fullname': 'Medewerker',
            'tasktype_general_name': 'Taaktype',
            'amount': 'Totaal uren'
        }),
        use_container_width=True
    )
    fig2 = px.bar(
        uren_per_maand_taak_med,
        x='maand',
        y='amount',
        color='tasktype_general_name',
        facet_row='fullname',
        title='Uren per taaktype per maand per medewerker',
        labels={'amount': 'Uren', 'maand': 'Maand', 'tasktype_general_name': 'Taaktype', 'fullname': 'Medewerker'},
        text_auto=True
    )
    fig2.update_layout(barmode='stack', xaxis_title='Maand', yaxis_title='Totaal uren', legend_title='Taaktype', template='plotly_white', margin=dict(l=40, r=40, t=60, b=40))
    st.plotly_chart(fig2, use_container_width=True, key="employee_tasktype_chart")
else:
    st.info("Selecteer één of meer medewerkers om hun detailoverzicht te zien.")

st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)
    
    

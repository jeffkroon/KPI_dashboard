import streamlit as st
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import plotly.express as px

# Data & omgeving setup
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

@st.cache_data(ttl=3600)
def load_data(table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, con=engine)

# Datasets laden
df_employees = load_data("employees")
df_employees['fullname'] = df_employees['firstname'] + " " + df_employees['lastname']
df_projects = load_data("projects")
df_companies = load_data("companies")
df_uren = load_data("urenregistratie")

# Filter niet-gearchiveerde projecten
df_projects = df_projects[df_projects["archived"] != True]

# Zorg dat totalexclvat numeriek is voor projecten
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")

# Projecten met klantnaam mergen
df_projects = df_projects.merge(
    df_companies[['id', 'companyname']], left_on='company_id', right_on='id', how='left', suffixes=('', '_company')
)

# Pagina titel
st.title("📋 Project Overzicht met Medewerker Uren")

# --- Filters bovenaan ---
project_options = df_projects['name'].unique().tolist()
# Alleen eerste 10 projecten standaard geselecteerd
default_projects = project_options[:10]

selected_projects = st.multiselect(
    "Selecteer één of meerdere projecten",
    options=project_options,
    default=default_projects,
    help="Gebruik het zoekveld om projecten te vinden"
)
# Filter uren op geselecteerde projecten (via offerprojectbase_id)
df_uren_filtered = df_uren[df_uren['offerprojectbase_id'].isin(df_projects[df_projects['name'].isin(selected_projects)]['id'])]

# Datumfilter (optioneel)
df_uren_filtered['date_date'] = pd.to_datetime(df_uren_filtered['date_date'], errors='coerce')
min_date = df_uren_filtered['date_date'].min()
max_date = df_uren_filtered['date_date'].max()
date_range = st.date_input("Selecteer datumrange urenregistratie", [min_date, max_date])
if len(date_range) == 2:
    start_date, end_date = date_range
    df_uren_filtered = df_uren_filtered[(df_uren_filtered['date_date'] >= pd.to_datetime(start_date)) & 
                                        (df_uren_filtered['date_date'] <= pd.to_datetime(end_date))]

# KPI-berekeningen
aantal_projecten = len(selected_projects)
totale_omzet = df_projects[df_projects['name'].isin(selected_projects)]['totalexclvat'].sum()

# Medewerkers betrokken bij geselecteerde projecten
medewerkers_ids = df_uren_filtered['employee_id'].unique()
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
    uren_per_medewerker = df_uren_filtered.groupby('employee_id')['amount'].sum()
    
    # Totale uren per medewerker over alle projecten (voor percentage)
    totale_uren_per_medewerker = df_uren.groupby('employee_id')['amount'].sum()
    
    df_medewerkers_filtered['Uren aan geselecteerde projecten'] = df_medewerkers_filtered['id'].map(uren_per_medewerker).fillna(0)
    df_medewerkers_filtered['Totale uren'] = df_medewerkers_filtered['id'].map(totale_uren_per_medewerker).fillna(0)
    df_medewerkers_filtered['Percentage uren'] = (df_medewerkers_filtered['Uren aan geselecteerde projecten'] / df_medewerkers_filtered['Totale uren']).fillna(0) * 100
    df_medewerkers_filtered['fullname'] = df_medewerkers_filtered['firstname'] + " " + df_medewerkers_filtered['lastname']
    
    # Optioneel: functie of uurtarief tonen als kolom als beschikbaar
    if 'function' in df_medewerkers_filtered.columns:
        df_medewerkers_filtered = df_medewerkers_filtered.rename(columns={'function': 'Functie'})
    else:
        df_medewerkers_filtered['Functie'] = "Onbekend"
    
    df_display = df_medewerkers_filtered[['fullname', 'Uren aan geselecteerde projecten', 'Percentage uren', 'Functie']].copy()
    df_display = df_display.sort_values(by='Uren aan geselecteerde projecten', ascending=False)
    
    st.subheader("👷 Medewerkers die aan geselecteerde projecten werken")
    st.dataframe(df_display.style.format({
        'Uren aan geselecteerde projecten': "{:,.2f}",
        'Percentage uren': "{:.1f} %"
    }))
else:
    st.info("Geen medewerkers gevonden die uren hebben geschreven aan de geselecteerde projecten.")

# Optionele filter op medewerkers binnen geselecteerde projecten
st.subheader("Filter medewerkers binnen geselecteerde projecten")
medewerker_opties = df_medewerkers_filtered['fullname'].tolist() if aantal_medewerkers > 0 else []
selected_medewerkers = st.multiselect("Selecteer medewerkers (optioneel)", options=medewerker_opties)

# Visualisaties
st.subheader("📊 Visualisaties")

if aantal_medewerkers > 0:
    df_vis = df_uren_filtered.copy()
    if selected_medewerkers:
        medewerker_ids_filter = df_employees[df_employees['fullname'].isin(selected_medewerkers)]['id']
        df_vis = df_vis[df_vis['employee_id'].isin(medewerker_ids_filter)]
    
    # Uren per medewerker bar chart
    uren_per_medewerker_vis = df_vis.groupby('employee_id')['amount'].sum()
    df_med_uren = df_employees.set_index('id').loc[uren_per_medewerker_vis.index]
    df_med_uren['Uren'] = uren_per_medewerker_vis.values
    df_med_uren['fullname'] = df_med_uren['firstname'] + " " + df_med_uren['lastname']
    
    fig1 = px.bar(df_med_uren.sort_values('Uren', ascending=True), x='Uren', y='fullname', orientation='h',
                  title='Uren per medewerker (filter toepasbaar)', labels={'Uren': 'Uren', 'fullname': 'Medewerker'},
                  color='fullname', color_discrete_sequence=px.colors.qualitative.Plotly, text_auto='.2s')
    fig1.update_layout(showlegend=False, template="plotly_white")
    st.plotly_chart(fig1, use_container_width=True)
    
    # Uren per taak bar chart
    if 'task_searchname' in df_uren_filtered.columns:
        uren_per_taak = df_vis.groupby('task_searchname')['amount'].sum()
        df_taak = uren_per_taak.reset_index().sort_values('amount', ascending=False).head(10)
        fig2 = px.bar(df_taak, x='task_searchname', y='amount',
                      title='Top 10 taken per uren', labels={'task_searchname': 'Taak', 'amount': 'Uren'},
                      color='task_searchname', color_discrete_sequence=px.colors.qualitative.Plotly, text_auto='.2s')
        fig2.update_layout(showlegend=False, xaxis_tickangle=-45, template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.info("Geen data beschikbaar voor visualisaties.")
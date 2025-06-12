import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime
# --- SETUP ---
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

st.set_page_config(
    page_title="Dunion KPI Dashboard",
    page_icon="images/dunion-logo-def_donker-06.png",
    layout="wide",
    initial_sidebar_state="expanded")
try:
    from streamlit_extras.metric_cards import style_metric_cards
except ImportError:
    st.warning("📛 'streamlit-extras' is niet geïnstalleerd of niet vindbaar door je environment.")
st.logo("images/dunion-logo-def_donker-06.png")
st.title("Dunion KPI Dashboard – Overzicht")

# --- LOAD DATA ---
@st.cache_data
def load_data(table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, con=engine)

df_projects = load_data("projects")
# Exclude archived projects
df_projects = df_projects[df_projects["archived"] != True]
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_employees = load_data("employees")
df_companies = load_data("companies")
df_uren = load_data("urenregistratie")
df_projectlines = load_data("projectlines_per_company")
# Filter verborgen regels uit df_projectlines
#df_projectlines = df_projectlines[df_projectlines["hidefortimewriting"].fillna(False) != True]
# Debug: Toon aantal verborgen regels

# Filter enkel projectlines van actieve projecten en met rowtype 'NORMAAL'
active_project_ids = df_projects["id"].tolist()
df_projectlines = df_projectlines[df_projectlines["offerprojectbase_id"].isin(active_project_ids)]
df_projectlines = df_projectlines[df_projectlines["rowtype_searchname"] == "NORMAAL"]

df_projectlines["amountwritten"] = pd.to_numeric(df_projectlines["amountwritten"], errors="coerce")

# Bereken omzet én uren per bedrijf op basis van projectlines
df_projectlines["werkelijke_opbrengst"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce") * df_projectlines["amountwritten"]
aggregatie_per_bedrijf = df_projectlines.groupby("bedrijf_id").agg({
    "werkelijke_opbrengst": "sum",
    "amountwritten": "sum"
}).reset_index()
aggregatie_per_bedrijf.columns = ["bedrijf_id", "werkelijke_opbrengst", "totaal_uren"]

# Voeg werkelijke omzet toe aan projects via een merge
df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="company_id", right_on="bedrijf_id", how="left")
df_projects["werkelijke_opbrengst"] = df_projects["werkelijke_opbrengst"].fillna(0)
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0)


# --- KPI CARDS ---
kpi_section = st.container()
with kpi_section:
    style_metric_cards(
        background_color="#F0F4F8",
        border_size_px=1,
        border_color="#BBB",
        border_radius_px=8,
        border_left_color="#9AD8E1",
        box_shadow=True
    )
    st.subheader("KPI's Overzicht")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        actieve_projecten = df_projects[df_projects["enddate_date"].isna()].shape[0]
        st.metric(label="📁 Actieve Projecten", value=actieve_projecten, delta=None, delta_color="normal", help=None, label_visibility="visible")
    with col2:
        huidig_jaar = datetime.now().year
        df_projects["startdate_dt"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
        projecten_dit_jaar = df_projects[df_projects["startdate_dt"].dt.year == huidig_jaar].shape[0]
        st.metric("📅 Projecten dit jaar", projecten_dit_jaar)
    with col3:
        uren = f"{df_projects['totaal_uren'].sum():.0f} uur"
        st.metric(label="⌛ Gewerkte Uren", value=uren, delta=None, delta_color="normal", help=None, label_visibility="visible")
    with col4:
        actieve_medewerkers = df_employees[df_employees["active"] == True].shape[0]
        st.metric(label="🧑 Actieve Medewerkers", value=actieve_medewerkers, delta=None, delta_color="normal", help=None, label_visibility="visible")

extra_kpis = st.container()
with extra_kpis:
    kpi5, kpi6 = st.columns(2)
    with kpi5:
        if not df_projects.empty:
            topklant_row = df_projects.groupby("company_searchname")["werkelijke_opbrengst"].sum().idxmax()
            topklant_omzet = df_projects.groupby("company_searchname")["werkelijke_opbrengst"].sum().max()
            st.metric("👑 Topklant Omzet", topklant_row, f"€ {topklant_omzet:,.2f}")
        else:
            st.metric("👑 Topklant Omzet", "N/A", "€ 0.00")
    with kpi6:
        totale_opbrengst = f"€ {df_projects['werkelijke_opbrengst'].sum():,.2f}"
        st.metric(label="💰 Totale Opbrengst", value=totale_opbrengst, delta=None, delta_color="normal", help=None, label_visibility="visible")


# --- CHARTS ---
st.subheader("📊 Inzichten")
tabs = st.tabs(["Status", "Topklanten", "Uren & Omzet", "Treemap", "Cumulatief"])

with tabs[0]:
    st.markdown("**🔄 Projectstatus (Fase)**")
    fase_counts = df_projects["phase_searchname"].value_counts().reset_index()
    fase_counts.columns = ["Fase", "Aantal"]
    fig1 = px.bar(fase_counts, x="Fase", y="Aantal", title="Aantal projecten per fase")
    st.plotly_chart(fig1, use_container_width=True)

with tabs[1]:
    st.markdown("**💼 Top 5 Klanten op Opbrengst**")
    omzet_per_klant = df_projects.groupby("company_searchname")["werkelijke_opbrengst"].sum().nlargest(5).reset_index()
    omzet_per_klant.columns = ["Klant", "Opbrengst"]
    fig2 = px.bar(omzet_per_klant, x="Opbrengst", y="Klant", orientation='h', title="Top 5 Klanten (opbrengst uit projectlines)")
    totaal_opbrengst = omzet_per_klant["Opbrengst"].sum()
    st.metric("📊 Totaal Opbrengst Top 5", f"€ {totaal_opbrengst:,.2f}")
    st.plotly_chart(fig2, use_container_width=True)

with tabs[2]:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📈 Uren per Maand**")
        df_uren["maand"] = pd.to_datetime(df_uren["date_date"]).dt.to_period("M").astype(str)
        uren_per_maand = df_uren.groupby("maand")["amount"].sum().reset_index()
        fig3 = px.line(uren_per_maand, x="maand", y="amount", title="Gewerkte uren per maand")
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        st.markdown("**📉 Omzettrend per Maand**")
        df_projects["maand"] = pd.to_datetime(df_projects["startdate_date"]).dt.to_period("M").astype(str)
        omzet_per_maand = df_projects.groupby("maand")["totalexclvat"].sum().reset_index()
        fig4 = px.line(omzet_per_maand, x="maand", y="totalexclvat", title="Omzet per maand")
        st.plotly_chart(fig4, use_container_width=True)

with tabs[3]:
    st.markdown("**📦 Omzet per klant als treemap**")
    omzet_per_klant_treemap = df_projects.groupby("company_searchname", as_index=False)["werkelijke_opbrengst"].sum()
    fig5 = px.treemap(omzet_per_klant_treemap, path=["company_searchname"], values="werkelijke_opbrengst", title="📦 Verdeling opbrengst per klant (treemap)")
    st.plotly_chart(fig5, use_container_width=True)

with tabs[4]:
    st.markdown("**📈 Cumulatieve omzetgroei door het jaar**")
    df_projects["startdate"] = pd.to_datetime(df_projects["startdate_date"])
    df_projects['jaar'] = df_projects['startdate'].dt.year
    df_projects['maand'] = df_projects['startdate'].dt.to_period('M').astype(str)
    omzet_growth = df_projects.groupby('maand')["werkelijke_opbrengst"].sum().cumsum().reset_index()
    fig6 = px.area(omzet_growth, x="maand", y="werkelijke_opbrengst", title="📈 Cumulatieve omzetgroei door het jaar")
    st.plotly_chart(fig6, use_container_width=True)


# --- PROJECTOVERZICHT MET FILTERS ---
with st.expander("📁 Projectoverzicht en filters", expanded=True):
    klant_filter = st.multiselect("Klant", options=df_projects["company_searchname"].unique())
    fase_filter = st.multiselect("Fase", options=df_projects["phase_searchname"].unique())
    datum_filter = st.date_input("Startdatum vanaf", value=None)

    filtered_df = df_projects.copy()
    if klant_filter:
        filtered_df = filtered_df[filtered_df["company_searchname"].isin(klant_filter)]
    if fase_filter:
        filtered_df = filtered_df[filtered_df["phase_searchname"].isin(fase_filter)]
    if datum_filter:
        filtered_df = filtered_df[pd.to_datetime(filtered_df["startdate_date"]) >= pd.to_datetime(datum_filter)]

    st.dataframe(filtered_df[["name", "company_searchname", "phase_searchname", "startdate_date", "totalexclvat"]])


st.subheader("📋 Projectregels per bedrijf")

bedrijf_namen = df_companies["companyname"].sort_values().unique().tolist()
gekozen_bedrijf = st.selectbox("📌 Selecteer een bedrijf om de projectlines te bekijken", bedrijf_namen)

bedrijf_info = df_companies[df_companies["companyname"] == gekozen_bedrijf].iloc[0]
bedrijf_id = bedrijf_info["id"]

projectlines = df_projectlines[df_projectlines["bedrijf_id"] == bedrijf_id]

if not projectlines.empty:
    st.write(f"### 📂 Projectlines voor bedrijf: {gekozen_bedrijf} (ID: {bedrijf_id})")

    display_cols = ["offerprojectbase_id", "searchname", "sellingprice", "amountwritten", "werkelijke_opbrengst"]
    projectlines_display = projectlines[display_cols].copy()
    projectlines_display["offerprojectbase_id"] = projectlines_display["offerprojectbase_id"].astype(str)

    projectlines_display["sellingprice"] = pd.to_numeric(projectlines_display["sellingprice"], errors="coerce").round(2)
    projectlines_display["amountwritten"] = pd.to_numeric(projectlines_display["amountwritten"], errors="coerce").round(2)
    projectlines_display["werkelijke_opbrengst"] = pd.to_numeric(projectlines_display["werkelijke_opbrengst"], errors="coerce").round(2)

    # Voeg totalen toe
    sellingprice_sum = pd.to_numeric(projectlines["sellingprice"], errors="coerce").sum()
    amountwritten_sum = pd.to_numeric(projectlines["amountwritten"], errors="coerce").sum()
    werkelijke_opbrengst_sum = pd.to_numeric(projectlines["werkelijke_opbrengst"], errors="coerce").sum()

    total_row = pd.DataFrame({
        "offerprojectbase_id": ["TOTAAL"],
        "searchname": ["TOTAAL"],
        "sellingprice": [round(sellingprice_sum, 2)],
        "amountwritten": [round(amountwritten_sum, 2)],
        "werkelijke_opbrengst": [round(werkelijke_opbrengst_sum, 2)]
    })

    projectlines_display = pd.concat([projectlines_display, total_row], ignore_index=True)

    # Zorg dat alle kolommen tekst zijn waar nodig om serialization errors te vermijden
    projectlines_display["offerprojectbase_id"] = projectlines_display["offerprojectbase_id"].astype(str)
    projectlines_display["searchname"] = projectlines_display["searchname"].astype(str)

    # Sorteer op werkelijke_opbrengst aflopend
    projectlines_display = projectlines_display.sort_values(by="werkelijke_opbrengst", ascending=False)

    st.dataframe(projectlines_display, use_container_width=True)
else:
    st.info("Geen projectregels gevonden voor dit bedrijf.")

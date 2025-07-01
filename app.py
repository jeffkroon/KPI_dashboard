import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import cast

st.set_page_config(
    page_title="Dunion KPI Dashboard",
    page_icon="images/dunion-logo-def_donker-06.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.logo("images/dunion-logo-def_donker-06.png")
st.title("Dunion KPI Dashboard – Overzicht")

@st.cache_data
def load_data(table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, con=engine)

pd.set_option('future.no_silent_downcasting', True)

# --- SETUP ---
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")
engine = create_engine(POSTGRES_URL)

try:
    from streamlit_extras.metric_cards import style_metric_cards
except ImportError:
    st.warning("📛 'streamlit-extras' is niet geïnstalleerd of niet vindbaar door je environment.")

# --- LOAD DATA ---
df_projects_raw: pd.DataFrame = pd.DataFrame(load_data("projects"))
df_companies: pd.DataFrame = pd.DataFrame(load_data("companies"))
df_employees: pd.DataFrame = pd.DataFrame(load_data("employees"))
df_projectlines: pd.DataFrame = pd.DataFrame(load_data("projectlines_per_company"))
df_invoices: pd.DataFrame = pd.DataFrame(load_data("invoices"))

# --- KPI CARDS ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🏢 Bedrijven", len(df_companies))
with col2:
    st.metric("📋 Projecten", len(df_projects_raw))
with col3:
    st.metric("📄 Facturen", len(df_invoices))
with col4:
    st.metric("⏰ Projectregels", len(df_projectlines))

st.markdown("---")

# --- DATA PREP ---
# Zorg dat de juiste kolommen bestaan en numeriek zijn
if 'bedrijf_id' not in df_projectlines.columns and 'company_id' in df_projectlines.columns:
    df_projectlines = df_projectlines.rename(columns={'company_id': 'bedrijf_id'})
if 'companyname' not in df_companies.columns and 'bedrijf_naam' in df_companies.columns:
    df_companies = df_companies.rename(columns={'bedrijf_naam': 'companyname'})
for col in ["amountwritten", "sellingprice"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Bereken totaal uren per bedrijf
df_uren_per_bedrijf = df_projectlines.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
df_uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Bereken totaal gefactureerd per bedrijf
factuurbedrag_per_bedrijf = (
    df_invoices[df_invoices["fase"] == "Factuur"]
    .copy()
    .assign(totalpayed=pd.to_numeric(df_invoices["totalpayed"], errors="coerce"))
    .groupby("company_id")[["totalpayed"]]
    .sum()
    .reset_index()
)
factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf.rename(columns={"company_id": "bedrijf_id"})

# Combineer stats per bedrijf
bedrijfsstats = df_uren_per_bedrijf.merge(factuurbedrag_per_bedrijf, on="bedrijf_id", how="outer")
bedrijfsstats = bedrijfsstats.merge(df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left")
bedrijfsstats = bedrijfsstats.drop(columns=[col for col in ['id'] if col in bedrijfsstats.columns])
bedrijfsstats["totaal_uren"] = bedrijfsstats["totaal_uren"].fillna(0)
bedrijfsstats["totalpayed"] = bedrijfsstats["totalpayed"].fillna(0)
bedrijfsstats["werkelijk_tarief_per_uur"] = bedrijfsstats["totalpayed"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)

# --- BAR CHART ---
st.subheader("📊 Werkelijk tarief per uur per bedrijf")
if not isinstance(bedrijfsstats, pd.DataFrame):
    chart_data = pd.DataFrame(bedrijfsstats)
else:
    chart_data = bedrijfsstats.copy()
assert isinstance(chart_data, pd.DataFrame), "chart_data moet een DataFrame zijn"
chart_data = chart_data[chart_data["werkelijk_tarief_per_uur"] > 0].sort_values(by="werkelijk_tarief_per_uur", ascending=False)  # type: ignore
fig = px.bar(
    chart_data,
    x="companyname",
    y="werkelijk_tarief_per_uur",
    labels={"companyname": "Bedrijf", "werkelijk_tarief_per_uur": "Werkelijk tarief per uur (€)"},
    title="Werkelijk tarief per uur per bedrijf",
    height=400
)
fig.update_layout(xaxis_tickangle=-45, margin=dict(l=40, r=20, t=60, b=120))
st.plotly_chart(fig, use_container_width=True)

# --- TOP 5 & BOTTOM 5 ---
st.markdown("### 🔝 Top 5 bedrijven – Hoog tarief per uur")
top5 = chart_data.head(5)[["companyname", "werkelijk_tarief_per_uur"]].copy()
top5["werkelijk_tarief_per_uur"] = pd.Series(top5["werkelijk_tarief_per_uur"]).apply(lambda x: f"€ {float(x):,.2f}")
top5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(top5, use_container_width=True)

st.markdown("### 📉 Bottom 5 bedrijven – Laag tarief per uur")
bottom5 = chart_data.sort_values(by="werkelijk_tarief_per_uur").head(5)[["companyname", "werkelijk_tarief_per_uur"]].copy()
bottom5["werkelijk_tarief_per_uur"] = pd.Series(bottom5["werkelijk_tarief_per_uur"]).apply(lambda x: f"€ {float(x):,.2f}")
bottom5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(bottom5, use_container_width=True)

# --- ZOEK & FACTUREN ---
st.markdown("---")
st.subheader("🔍 Selecteer een bedrijf voor details en facturen")
bedrijf_opties = bedrijfsstats["companyname"].dropna().unique().tolist()
bedrijf_naam_selectie = st.selectbox("Kies een bedrijf:", bedrijf_opties)
bedrijf_id_selectie = bedrijfsstats.loc[bedrijfsstats["companyname"] == bedrijf_naam_selectie, "bedrijf_id"].iloc[0] if bedrijf_naam_selectie else None

if bedrijf_naam_selectie:
    display_df = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_naam_selectie][["bedrijf_id", "companyname", "totalpayed", "totaal_uren", "werkelijk_tarief_per_uur"]].copy()
    assert isinstance(display_df, pd.DataFrame), "display_df moet een DataFrame zijn"
    display_df = display_df.rename(columns={
        "bedrijf_id": "Bedrijf ID",
        "companyname": "Bedrijfsnaam",
        "totalpayed": "Totaal Gefactureerd (€)",
        "totaal_uren": "Totaal Uren",
        "werkelijk_tarief_per_uur": "Werkelijk tarief per Uur (€)"
    })
    display_df["Totaal Gefactureerd (€)"] = display_df["Totaal Gefactureerd (€)"].apply(lambda x: f"€ {float(x):,.2f}")
    display_df["Werkelijk tarief per Uur (€)"] = display_df["Werkelijk tarief per Uur (€)"].apply(lambda x: f"€ {float(x):,.2f}")
    st.dataframe(display_df, use_container_width=True)

# --- FACTUREN PER BEDRIJF ---
st.markdown("---")
st.subheader("📄 Facturen van geselecteerd bedrijf")
if bedrijf_naam_selectie and bedrijf_id_selectie is not None:
    facturen_bedrijf = df_invoices[(df_invoices["company_id"] == bedrijf_id_selectie) & (df_invoices["status_searchname"] == "Verzonden")].copy()
    if not facturen_bedrijf.empty:
        facturen_bedrijf["totalpayed"] = pd.to_numeric(facturen_bedrijf["totalpayed"], errors="coerce")
        display_columns = ["number", "date_date", "status_searchname", "totalpayed", "subject"]
        display_df = facturen_bedrijf[display_columns].copy()
        assert isinstance(display_df, pd.DataFrame), "display_df moet een DataFrame zijn"
        display_df.columns = ["Factuurnummer", "Datum", "Status", "Bedrag (€)", "Onderwerp"]
        display_df["Bedrag (€)"] = display_df["Bedrag (€)"].apply(lambda x: f"€ {x:,.2f}" if pd.notna(x) else "€ 0.00")
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info(f"Geen facturen gevonden voor {bedrijf_naam_selectie}.")

# --- FOOTER ---
st.markdown("""<div style='text-align: right; color: #888; font-size: 0.9em;'>Dunion Dashboard &copy; 2024</div>""", unsafe_allow_html=True)

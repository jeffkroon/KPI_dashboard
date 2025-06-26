import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime

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
df_projects_raw = pd.DataFrame(load_data("projects"))
df_companies = pd.DataFrame(load_data("companies"))

# Merge projects with companies to get companyname
df_projects_raw = df_projects_raw.merge(
    df_companies[["id", "companyname"]],
    left_on="company_id",
    right_on="id",
    how="left",
    suffixes=("", "_bedrijf")
)

# Load invoices
df_invoices = pd.DataFrame(load_data("invoices"))
df_invoices.columns
# 🔥 pas hier filter je projecten
df_projects = df_projects_raw[df_projects_raw["archived"] != True].copy()

df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_projects["startdate_date"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
df_projects["enddate_date"] = pd.to_datetime(df_projects["enddate_date"], errors="coerce")

df_employees = pd.DataFrame(load_data("employees"))
df_uren = pd.DataFrame(load_data("urenregistratie"))
df_projectlines = pd.DataFrame(load_data("projectlines_per_company"))

# Filter projectlines op actieve projecten en 'NORMAAL'
active_project_ids = df_projects["id"].tolist()
df_projectlines = df_projectlines[df_projectlines["offerprojectbase_id"].isin(active_project_ids)].copy()
df_projectlines = df_projectlines[df_projectlines["rowtype_searchname"] == "NORMAAL"].copy()
df_projectlines.columns
# Numerieke kolommen netjes maken
for col in ["amountwritten", "sellingprice"]:
    df_projectlines.loc[:, col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Bereken totaal uren per bedrijf
aggregatie_per_bedrijf = pd.DataFrame(df_projectlines.groupby("bedrijf_id").agg({
    "amountwritten": "sum"
}).reset_index().copy())
aggregatie_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Merge in projecten
df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="company_id", right_on="bedrijf_id", how="left").copy()
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0).infer_objects(copy=False)

# --- UI ---
st.subheader("Factuurregels per bedrijf zoeken")

bedrijf_zoek = st.text_input("Zoek op bedrijfsnaam:")

if bedrijf_zoek:
    gefilterde_bedrijven = df_companies[df_companies["companyname"].str.contains(bedrijf_zoek, case=False, na=False)]
else:
    gefilterde_bedrijven = df_companies

bedrijf_opties = gefilterde_bedrijven["companyname"].tolist()
bedrijf_naam = st.selectbox("Kies een bedrijf:", bedrijf_opties) if bedrijf_opties else None

if bedrijf_naam:
    bedrijf_id = gefilterde_bedrijven.loc[gefilterde_bedrijven["companyname"] == bedrijf_naam, "id"].iloc[0]
    st.write(f"Bedrijf ID: {bedrijf_id}")

    # Filter invoices for this company: only with fase == "Factuur" and isbasis != True
    facturen_bedrijf = df_invoices[
        (df_invoices["company_id"] == bedrijf_id) &
        (df_invoices["fase"] == "Factuur").copy()]
    facturen_bedrijf["totalpayed"] = pd.to_numeric(facturen_bedrijf["totalpayed"], errors="coerce")
    st.subheader(f"Facturen voor {bedrijf_naam}")
    totaal_factuurbedrag = facturen_bedrijf["totalpayed"].sum()

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Aantal facturen", len(facturen_bedrijf))
    with col2:
        st.metric("Totale gefactureerde waarde", f"€ {totaal_factuurbedrag:,.2f}")

    st.dataframe(facturen_bedrijf[[
        "id", "company_searchname", "company_id", "number", "date_date",
        "status_searchname", "totalinclvat", "invoicelines", "tags", "fase", "totalpayed", "subject"
    ]])

    factuurregels_bedrijf = pd.DataFrame()  # Geen factuurregels beschikbaar
    st.info("Factuurregels zijn niet langer beschikbaar in dit dashboard.")
    
    
    #project = number, de rest = company_id
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
    initial_sidebar_state="expanded")

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
df_projects = pd.DataFrame(load_data("projects"))
df_companies = pd.DataFrame(load_data("companies"))

df_projects = df_projects.merge(
    df_companies[["id", "companyname"]],
    left_on="company_id",
    right_on="id",
    how="left",
    suffixes=("", "_bedrijf")
)

df_invoicelines = pd.DataFrame(load_data("invoicelines"))
df_invoicelines_merged = df_invoicelines.merge(
    df_projects[["id", "company_id", "companyname"]],
    left_on="project_id",
    right_on="id",
    how="left",
    suffixes=("", "_project")
)

df_projects = df_projects[df_projects["archived"] != True].copy()
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_projects["startdate_date"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
df_projects["enddate_date"] = pd.to_datetime(df_projects["enddate_date"], errors="coerce")

df_employees = pd.DataFrame(load_data("employees"))
df_uren = pd.DataFrame(load_data("urenregistratie"))
df_projectlines = pd.DataFrame(load_data("projectlines_per_company"))
# Filter alleen projectlines voor actieve projecten en rowtype 'NORMAAL'
active_project_ids = df_projects["id"].tolist()
df_projectlines = df_projectlines[df_projectlines["offerprojectbase_id"].isin(active_project_ids)].copy()
df_projectlines = df_projectlines[df_projectlines["rowtype_searchname"] == "NORMAAL"].copy()
# Zet numerieke kolommen om naar numeriek (float)
for col in ["amountwritten", "sellingprice"]:
    df_projectlines.loc[:, col] = pd.to_numeric(df_projectlines[col], errors="coerce")  # type: ignore
# Bereken werkelijke opbrengst zonder afronding
aggregatie_per_bedrijf = pd.DataFrame(df_projectlines.groupby("bedrijf_id").agg({  # type: ignore
    "amountwritten": "sum"
}).reset_index().copy())
aggregatie_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]
# Merge met projecten en vul NaN's op met 0
df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="company_id", right_on="bedrijf_id", how="left").copy()
# Vul NaN met 0 en converteer naar numeriek met infer_objects(copy=False)
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0).infer_objects(copy=False)
st.write(df_projects[df_projects['id'] == 330])
st.write(df_projectlines[df_projectlines['offerprojectbase_id'] == 330])
st.write(df_invoicelines[df_invoicelines['project_id'] == 330])
# Merge invoicelines met projectlines op project_id
# Dit geeft je toegang tot projectinformatie bij elke factuurregel
# Suffixes voorkomen kolomnaam-conflicten

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
    
    # Filter de factuurregels per geselecteerd bedrijf
    factuurregels_bedrijf = df_invoicelines_merged[df_invoicelines_merged["company_id"] == bedrijf_id]

    st.subheader(f"Factuurregels voor {bedrijf_naam}")
    st.dataframe(factuurregels_bedrijf[["project_id", "companyname", "description", "amount", "sellingprice"]])


# --- Toevoeging: Zoekbare tabel voor factuurregels per bedrijf ---
st.subheader("Factuurregels zoeken per bedrijf – Tabeloverzicht")

zoek_bedrijf_tabel = st.text_input("Zoek bedrijfsnaam in tabelweergave:")

if zoek_bedrijf_tabel:
    gefilterd_df = df_invoicelines_merged[df_invoicelines_merged["companyname"].str.contains(zoek_bedrijf_tabel, case=False, na=False)]
else:
    gefilterd_df = df_invoicelines_merged

st.dataframe(gefilterd_df[["companyname", "project_id", "description", "amount", "sellingprice"]])
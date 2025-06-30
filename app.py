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

# Merge projects with companies to get companyname
df_projects_raw = df_projects_raw.merge(
    df_companies[["id", "companyname"]],
    left_on="company_id",
    right_on="id",
    how="left",
    suffixes=("", "_bedrijf")
)

# Load invoices
df_invoices: pd.DataFrame = pd.DataFrame(load_data("invoices"))
# 🔥 pas hier filter je projecten
df_projects: pd.DataFrame = df_projects_raw.copy()

df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_projects["startdate_date"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
df_projects["enddate_date"] = pd.to_datetime(df_projects["enddate_date"], errors="coerce")

df_employees: pd.DataFrame = pd.DataFrame(load_data("employees"))
df_projectlines: pd.DataFrame = pd.DataFrame(load_data("projectlines_per_company"))
# Debug info
print("df_projects columns:", df_projects.columns.tolist())
print("df_projects dtypes:\n", df_projects.dtypes)
print("df_companies columns:", df_companies.columns.tolist())
print("df_companies dtypes:\n", df_companies.dtypes)
print("df_projectlines columns:", df_projectlines.columns.tolist())
print("df_projectlines dtypes:\n", df_projectlines.dtypes)
# Filter projectlines op actieve projecten en 'NORMAAL'
active_project_ids = df_projects["id"].tolist()
df_projectlines = cast(pd.DataFrame, df_projectlines[df_projectlines["offerprojectbase_id"].isin(active_project_ids)].copy())
df_projectlines = cast(pd.DataFrame, df_projectlines[df_projectlines["rowtype_searchname"] == "NORMAAL"].copy())
df_projectlines = cast(pd.DataFrame, df_projectlines.merge(
    df_companies,
    left_on="bedrijf_id",
    right_on="id",
    how="left",
    suffixes=("", "_bedrijf")
).copy())

# Numerieke kolommen netjes maken
for col in ["amountwritten", "sellingprice"]:
    df_projectlines.loc[:, col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Bereken totaal uren per bedrijf
aggregatie_per_bedrijf = pd.DataFrame(df_projectlines.groupby("bedrijf_id").agg({
    "amountwritten": "sum"
}).reset_index().copy())
aggregatie_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Merge in projecten

df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="id_bedrijf", right_on="bedrijf_id", how="left").copy()
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0).infer_objects(copy=False)

factuurbedrag_per_bedrijf = (
    df_invoices[df_invoices["fase"] == "Factuur"]
    .copy()
    .assign(totalpayed=pd.to_numeric(df_invoices["totalpayed"], errors="coerce"))
    .groupby("company_id")[["totalpayed"]]
    .sum()
    .reset_index()
)

# 🔁 Merge uren en facturen
bedrijfsstats: pd.DataFrame = aggregatie_per_bedrijf.merge(
    factuurbedrag_per_bedrijf, left_on="bedrijf_id", right_on="company_id", how="outer"
)

# 🔠 Voeg namen toe
bedrijfsstats = bedrijfsstats.merge(
    df_companies[["id", "companyname"]],
    left_on="bedrijf_id",
    right_on="id",
    how="left"
)

# Voeg deze regel toe direct NA het vullen van bedrijfsstats, VOOR selectie/rename van kolommen:
bedrijfsstats["werkelijke_tarief_per_uur"] = bedrijfsstats.apply(
    lambda row: row["totalpayed"] / row["totaal_uren"] if row["totaal_uren"] > 0 else 0,
    axis=1
)

# Daarna pas kolommen selecteren of hernoemen voor presentatie

# Add some basic metrics at the top
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🏢 Bedrijven", len(df_companies))
with col2:
    st.metric("📋 Projecten", len(df_projects))
with col3:
    st.metric("📄 Facturen", len(df_invoices))
with col4:
    st.metric("⏰ Projectregels", len(df_projectlines))

st.markdown("---")

st.subheader("📈 KPI Analyse per Bedrijf")

gemiddelde_tarief = bedrijfsstats["werkelijke_tarief_per_uur"].mean()
hoogste_tarief = bedrijfsstats["werkelijke_tarief_per_uur"].max()
laagste_tarief = bedrijfsstats["werkelijke_tarief_per_uur"].min()

st.markdown(f"📊 Gemiddeld tarief per uur: **€ {gemiddelde_tarief:,.2f}**")
st.markdown(f"🥇 Hoogste tarief per uur: **€ {hoogste_tarief:,.2f}**")
st.markdown(f"🥶 Laagste tarief per uur: **€ {laagste_tarief:,.2f}**")

topbedrijven = bedrijfsstats.copy()
topbedrijven = topbedrijven.sort_values(by="werkelijke_tarief_per_uur", ascending=False).reset_index(drop=True)

# Voeg kleurcode toe voor traffic light effect
def kleurencode(tarief):
    if tarief >= 75:
        return "🟢"
    elif tarief >= 40:
        return "🟡"
    else:
        return "🔴"

topbedrijven["⚡ Tariefstatus"] = topbedrijven["werkelijke_tarief_per_uur"].apply(kleurencode)

display_kpis = topbedrijven[["companyname", "totalpayed", "totaal_uren", "werkelijke_tarief_per_uur", "⚡ Tariefstatus"]].copy()
display_kpis.columns = ["Bedrijfsnaam", "Totaal Gefactureerd (€)", "Totaal Uren", "Tarief per Uur (€)", "Tariefstatus"]

display_kpis["Totaal Gefactureerd (€)"] = display_kpis["Totaal Gefactureerd (€)"].apply(
    lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
)
display_kpis["Tarief per Uur (€)"] = display_kpis["Tarief per Uur (€)"].apply(
    lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
)

st.markdown("### 🔝 Top 5 bedrijven – Hoog tarief per uur")
top5 = topbedrijven[topbedrijven["werkelijke_tarief_per_uur"] > 0].dropna(subset=["werkelijke_tarief_per_uur"]).head(5)[["companyname", "werkelijke_tarief_per_uur"]].copy()
top5["werkelijke_tarief_per_uur"] = top5["werkelijke_tarief_per_uur"].apply(lambda x: f"€ {float(x):,.2f}")
top5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(top5, use_container_width=True)

st.markdown("### 📉 Bottom 5 bedrijven – Laag tarief per uur")
bottom5 = topbedrijven[topbedrijven["werkelijke_tarief_per_uur"] > 0].dropna(subset=["werkelijke_tarief_per_uur"]).sort_values(by="werkelijke_tarief_per_uur").head(5)[["companyname", "werkelijke_tarief_per_uur"]].copy()
bottom5["werkelijke_tarief_per_uur"] = bottom5["werkelijke_tarief_per_uur"].apply(lambda x: f"€ {float(x):,.2f}")
bottom5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(bottom5, use_container_width=True)

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

    # Filter invoices for this company: only with fase == "Factuur"
    facturen_bedrijf = df_invoices[
        (df_invoices["company_id"] == bedrijf_id) &
        (df_invoices["fase"] == "Factuur")
    ].copy()
    
    # Fix the SettingWithCopyWarning by using loc
    facturen_bedrijf.loc[:, "totalpayed"] = pd.to_numeric(facturen_bedrijf["totalpayed"], errors="coerce")
    
    st.subheader(f"Facturen voor {bedrijf_naam}")
    totaal_factuurbedrag = facturen_bedrijf["totalpayed"].sum()

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Aantal facturen", len(facturen_bedrijf))
    with col2:
        st.metric("Totale gefactureerde waarde", f"€ {totaal_factuurbedrag:,.2f}")

    # Clean up the display columns
    display_columns = ["number", "date_date", "status_searchname", "totalpayed", "subject"]
    display_df = cast(pd.DataFrame, facturen_bedrijf[display_columns].copy())
    display_df.columns = ["Factuurnummer", "Datum", "Status", "Bedrag (€)", "Onderwerp"]
    display_df["Bedrag (€)"] = display_df["Bedrag (€)"].apply(lambda x: f"€ {x:,.2f}" if pd.notna(x) else "€ 0.00")
    
    st.dataframe(display_df, use_container_width=True)

    st.info("📝 Factuurregels zijn niet langer beschikbaar in dit dashboard.")

# 🎯 Final columns

# --- Zoek/filter sectie in bedrijfsstats ---
st.subheader("🔍 Zoek in samenvattende bedrijfsdata")

# Add some spacing and styling
st.markdown("---")

col1, col2 = st.columns([2, 1])
with col1:
    bedrijfszoek = st.text_input("Zoek bedrijfsnaam in overzicht:", placeholder="Type bedrijfsnaam...")

with col2:
    if st.button("🔍 Zoeken", type="primary"):
        pass

if bedrijfszoek:
    zoekresultaat = cast(pd.DataFrame, bedrijfsstats[bedrijfsstats["companyname"].str.contains(bedrijfszoek, case=False, na=False)].copy())
    if len(zoekresultaat) > 0:
        st.success(f"✅ {len(zoekresultaat)} bedrijf(en) gevonden")
        
        # Selecteer de juiste kolommen vóór het hernoemen
        display_zoekresultaat = zoekresultaat[["bedrijf_id", "companyname", "totalpayed", "totaal_uren", "werkelijke_tarief_per_uur"]].copy()
        display_zoekresultaat.columns = ["Bedrijf ID", "Bedrijfsnaam", "Totaal Gefactureerd (€)", "Totaal Uren", "Werkelijk Tarief per Uur (€)"]
        
        # Safe formatting for currency columns
        display_zoekresultaat["Totaal Gefactureerd (€)"] = display_zoekresultaat["Totaal Gefactureerd (€)"].apply(
            lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
        )
        display_zoekresultaat["Werkelijk Tarief per Uur (€)"] = display_zoekresultaat["Werkelijk Tarief per Uur (€)"].apply(
            lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
        )
        st.dataframe(display_zoekresultaat, use_container_width=True)
    else:
        st.warning("⚠️ Geen bedrijven gevonden met deze zoekterm")
else:
    st.info("📊 Toon alle bedrijven")
    # Selecteer de juiste kolommen vóór het hernoemen
    display_bedrijfsstats = bedrijfsstats[["bedrijf_id", "companyname", "totalpayed", "totaal_uren", "werkelijke_tarief_per_uur"]].copy()
    display_bedrijfsstats.columns = ["Bedrijf ID", "Bedrijfsnaam", "Totaal Gefactureerd (€)", "Totaal Uren", "Werkelijk Tarief per Uur (€)"]
    # Safe formatting for currency columns
    display_bedrijfsstats["Totaal Gefactureerd (€)"] = display_bedrijfsstats["Totaal Gefactureerd (€)"].apply(
        lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
    )
    display_bedrijfsstats["Werkelijk Tarief per Uur (€)"] = display_bedrijfsstats["Werkelijk Tarief per Uur (€)"].apply(
        lambda x: f"€ {float(x):,.2f}" if pd.notna(x) and x != 0 else "€ 0.00"
    )
    st.dataframe(display_bedrijfsstats, use_container_width=True)

df_projectlines["bedrijf_id"] = df_projectlines["bedrijf_id"].astype("Int64")
df_companies["id"] = df_companies["id"].astype("Int64")



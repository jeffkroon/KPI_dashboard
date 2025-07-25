import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import altair as alt
from openai import OpenAI
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS
from utils.data_loaders import load_data, load_data_df

st.set_page_config(
    page_title="Customer-analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- AUTHENTICATIE ---
require_login()
require_email_whitelist(ALLOWED_EMAILS)

# --- LOGOUT IN SIDEBAR ---
if "access_token" in st.session_state:
    st.sidebar.write(f"Ingelogd als: {st.session_state.get('user_email', '')}")
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

st.logo("images/dunion-logo-def_donker-06.png")

st.title("projectrendement")

st.markdown("## 🔎 Filter bedrijven op type")

filter_keuze = st.radio(
    "Selecteer een bedrijfstype",
    ("Alle bedrijven", "Eigen bedrijven", "Klanten"),
    horizontal=True
)

def bedrijf_heeft_tag(tag_string, filter_primary_tag):
    if not isinstance(tag_string, str):
        return False
    tags = [t.strip() for t in tag_string.split(",")]
    return filter_primary_tag in tags

# Tags logica
eigen_tag = "1 | Eigen webshop(s) / bedrijven"
klant_tag = "1 | Externe opdrachten / contracten"

from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")

# === OMZET OPTIE SELECTIE ===
omzet_optie = st.radio("📊 Welke omzet wil je tonen?", options=["Werkelijke omzet (facturen)", "Geplande omzet (offerte)"], index=0, horizontal=True)

# === ARCHIVEER FILTER SELECTIE ===
toon_archived = st.radio(
    "📁 Wil je ook gearchiveerde projecten meenemen?",
    options=["Nee, alleen actieve projecten", "Ja, ook gearchiveerde projecten"],
    index=0,
    horizontal=True
)

# --- DATA EXACT ZOALS IN app.py ---
df_projects_raw = load_data_df("projects", columns=["id", "company_id", "archived", "totalexclvat"])
if not isinstance(df_projects_raw, pd.DataFrame):
    df_projects_raw = pd.concat(list(df_projects_raw), ignore_index=True)
# --- Geplande omzet per bedrijf toevoegen direct na conversie naar DataFrame
df_projects_raw["totalexclvat"] = pd.to_numeric(df_projects_raw["totalexclvat"], errors="coerce").fillna(0)

# === ARCHIVEER FILTERING OP PROJECTEN ===
if toon_archived == "Nee, alleen actieve projecten":
    df_projects_filtered = df_projects_raw[df_projects_raw["archived"] != True].copy()
else:
    df_projects_filtered = df_projects_raw.copy()

# Geplande omzet per bedrijf op basis van gefilterde projecten
geplande_omzet_per_bedrijf = df_projects_filtered.groupby("company_id")["totalexclvat"].sum().reset_index()
geplande_omzet_per_bedrijf.rename(columns={"company_id": "bedrijf_id", "totalexclvat": "geplande_omzet"}, inplace=True)
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

filter_primary_tag = None
if filter_keuze == "Eigen bedrijven":
    filter_primary_tag = eigen_tag
    df_companies = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, filter_primary_tag))]
elif filter_keuze == "Klanten":
    filter_primary_tag = klant_tag
    df_companies = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, filter_primary_tag))]
# Bij 'Alle bedrijven' nemen we alle bedrijven met een tag
elif filter_keuze == "Alle bedrijven":
    df_companies = df_companies[df_companies["tag_names"].notnull()]

bedrijf_ids = df_companies["id"].tolist()
# Debug: Toon filtering resultaat
st.info(f"✅ Filtering actief: {len(bedrijf_ids)} bedrijven geselecteerd na filtering op '{filter_keuze}'.")
df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
if not isinstance(df_employees, pd.DataFrame):
    df_employees = pd.concat(list(df_employees), ignore_index=True)
# --- Projectlines laden ---
df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "amountwritten", "sellingprice", "amount", "unit_searchname", "hidefortimewriting", "offerprojectbase_id"])
if not isinstance(df_projectlines, pd.DataFrame):
    df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
df_projectlines = df_projectlines[df_projectlines["bedrijf_id"].isin(bedrijf_ids)]

# --- Filter projectlines op project_id's van zichtbare projecten ---
project_ids = df_projects_filtered["id"].unique().tolist()
if "offerprojectbase_id" in df_projectlines.columns:
    df_projectlines = df_projectlines[df_projectlines["offerprojectbase_id"].isin(project_ids)].copy()
# Filter alleen urenregels voor analyses van uren
df_projectlines_uren = df_projectlines[
    (df_projectlines["unit_searchname"].str.lower() == "uur") &
    (df_projectlines["hidefortimewriting"] == False)
].copy()
# Laat alle regels staan voor omzetanalyses
df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "subject"])
if not isinstance(df_invoices, pd.DataFrame):
    df_invoices = pd.concat(list(df_invoices), ignore_index=True)
df_invoices = df_invoices[df_invoices["company_id"].isin(bedrijf_ids)]

# Kolomhernoemingen en numerieke conversies
if 'bedrijf_id' not in df_projectlines.columns and 'company_id' in df_projectlines.columns:
    df_projectlines = df_projectlines.rename(columns={'company_id': 'bedrijf_id'})
if 'companyname' not in df_companies.columns and 'bedrijf_naam' in df_companies.columns:
    df_companies = df_companies.rename(columns={'bedrijf_naam': 'companyname'})
for col in ["amountwritten", "sellingprice"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

#
# Bereken totaal uren per bedrijf op basis van projectlines_uren (inclusief filtering op archived/unit/hidefortimewriting)
uren_per_bedrijf = df_projectlines_uren.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Bereken totaal gefactureerd per bedrijf direct in SQL
factuurbedrag_per_bedrijf = load_data_df("invoices", columns=["company_id", "SUM(CAST(totalpayed AS FLOAT)) as totalpayed"], where="fase = 'Factuur'", group_by="company_id")
factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf.rename(columns={"company_id": "bedrijf_id"})
factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf[factuurbedrag_per_bedrijf["bedrijf_id"].isin(bedrijf_ids)]

# Zorg dat beide DataFrames een kolom 'bedrijf_id' hebben vóór de merge
if 'company_id' in uren_per_bedrijf.columns:
    uren_per_bedrijf = uren_per_bedrijf.rename(columns={'company_id': 'bedrijf_id'})
if 'company_id' in factuurbedrag_per_bedrijf.columns:
    factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf.rename(columns={'company_id': 'bedrijf_id'})

 # Combineer stats per bedrijf
bedrijfsstats = uren_per_bedrijf.merge(factuurbedrag_per_bedrijf, on="bedrijf_id", how="outer")
bedrijfsstats = bedrijfsstats.merge(df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left")
bedrijfsstats = bedrijfsstats.drop(columns=[col for col in ['id'] if col in bedrijfsstats.columns])
bedrijfsstats["totaal_uren"] = bedrijfsstats["totaal_uren"].fillna(0)
bedrijfsstats["totalpayed"] = bedrijfsstats["totalpayed"].fillna(0)
# Voeg geplande_omzet toe aan bedrijfsstats en vul NaN met 0
bedrijfsstats = bedrijfsstats.merge(geplande_omzet_per_bedrijf, on="bedrijf_id", how="left")
bedrijfsstats["geplande_omzet"] = bedrijfsstats["geplande_omzet"].fillna(0)
# Zorg dat totalpayed en totaal_uren numeriek zijn vóór deling
bedrijfsstats["totalpayed"] = pd.to_numeric(bedrijfsstats["totalpayed"], errors="coerce").fillna(0)
bedrijfsstats["totaal_uren"] = pd.to_numeric(bedrijfsstats["totaal_uren"], errors="coerce").fillna(0)
# Dynamische berekening tarief_per_uur op basis van omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["totalpayed"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)
else:
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["geplande_omzet"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)

# --- Pas filtering toe op bedrijfsstats direct na merge ---
bedrijfsstats = bedrijfsstats[bedrijfsstats["bedrijf_id"].isin(bedrijf_ids)].copy()

#
# Bereken gemiddeld tarief per klant (bedrijf) -- alleen op urenregels
df_projectlines_uren["sellingprice"] = pd.to_numeric(df_projectlines_uren["sellingprice"], errors="coerce")
df_projectlines_uren = df_projectlines_uren[df_projectlines_uren["sellingprice"].notna()]
gemiddeld_tarief_per_klant = df_projectlines_uren.groupby('bedrijf_id')["sellingprice"].mean().reset_index()
gemiddeld_tarief_per_klant.columns = ["bedrijf_id", "gemiddeld_tarief"]
bedrijfsstats = bedrijfsstats.merge(gemiddeld_tarief_per_klant, on="bedrijf_id", how="left")

# Nu kun je bedrijfsstats gebruiken voor verdere analyses en visualisaties, net als in app.py
# De rest van de analyses en visualisaties kun je nu baseren op bedrijfsstats
# ... bestaande analyses en visualisaties ...

# Filter bedrijven met daadwerkelijk gewerkte uren
bedrijfsstats = bedrijfsstats[bedrijfsstats["totaal_uren"] > 0].copy()

# === Alleen uren van projectonderdelen met unit_searchname == 'uur' ===
df_projectlines_unit = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "amountwritten", "unit_searchname"])
if not isinstance(df_projectlines_unit, pd.DataFrame):
    df_projectlines_unit = pd.concat(list(df_projectlines_unit), ignore_index=True)
df_projectlines_unit = df_projectlines_unit[df_projectlines_unit["bedrijf_id"].isin(bedrijf_ids)]
df_projectlines_uur = df_projectlines_unit[df_projectlines_unit["unit_searchname"].str.lower() == "uur"].copy()
uren_per_bedrijf_uur = df_projectlines_uur.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
uren_per_bedrijf_uur.columns = ["bedrijf_id", "totaal_uren_uur"]

# Merge deze gefilterde uren met bedrijfsstats
bedrijfsstats = bedrijfsstats.merge(uren_per_bedrijf_uur, on="bedrijf_id", how="left")

# Bereken rendement per uur per bedrijf (voor backward compatibility, maar niet meer gebruiken)
bedrijfsstats["rendement_per_uur"] = (
    bedrijfsstats["totalpayed"] / bedrijfsstats["totaal_uren"]
).round(2)



# Voeg werkelijke omzet toe aan projects via een merge
df_projects_filtered = df_projects_filtered.merge(bedrijfsstats, left_on="company_id", right_on="bedrijf_id", how="left")
df_projects_filtered["werkelijke_opbrengst"] = df_projects_filtered["totalpayed"].fillna(0)
df_projects_filtered["totaal_uren"] = df_projects_filtered["totaal_uren"].fillna(0)


# Sorteer en filter op tarief_per_uur, hoogste eerst, filter 0 en NaN
df_rend = bedrijfsstats.copy()
df_rend = df_rend.dropna(subset=["tarief_per_uur"]).copy()  # type: ignore
df_rend = df_rend[df_rend["tarief_per_uur"] > 0].copy()  # type: ignore
df_rend = df_rend.sort_values("tarief_per_uur", ascending=False).copy()  # type: ignore

# === RISICOCATEGORIE TOEVOEGEN OP BASIS VAN RENDEMENT EN TIJDSBESTEDING ===
# Removed categoriseer_risico function and its apply call per instructions

# KPI-cards: top 3 bedrijven met hoogste tarief per uur (dynamisch label)
titel_tarief = "Werkelijk uurtarief" if omzet_optie == "Werkelijke omzet (facturen)" else "Gepland uurtarief"
# Dynamische omzetkolom op basis van omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    omzet_kolom = "totalpayed"
else:
    omzet_kolom = "geplande_omzet"
st.markdown(f"### 🥇 Top 3 bedrijven op basis van {titel_tarief}")
cols = st.columns(3)
# Let op: afronden alleen bij presentatie, niet in berekening!
for i, (_, row) in enumerate(df_rend.head(3).iterrows()):
    cols[i].metric(
        label=f"{row['companyname']}",
        value=f"€ {row['tarief_per_uur']:.2f}/uur"
    )

# KPI-cards: bottom 10 bedrijven met laagste tarief per uur (dynamisch label)
st.markdown(f"### 🛑 Bottom 10 bedrijven op basis van {titel_tarief}")
df_bottom_10 = df_rend.nsmallest(10, "tarief_per_uur")[["companyname", "totaal_uren", "totalpayed", "tarief_per_uur"]]
# Voor tabellen waar kolomnamen worden toegewezen
df_bottom_10.columns = ["Bedrijf", "Totaal Uren", "Opbrengst", titel_tarief]
# Alleen afronden in presentatie, niet in data!
st.dataframe(df_bottom_10.style.format({
    "Totaal Uren": "{:.1f}",
    "Opbrengst": "€ {:.2f}",
    titel_tarief: "€ {:.2f}"
}), use_container_width=True)

# Extra inzichten: gemiddeld tarief, mediaan, en aantal bedrijven onder drempel
gemiddeld_tarief = df_rend["tarief_per_uur"].mean()
mediaan_tarief = df_rend["tarief_per_uur"].median()
ondergrens = 75  # drempelrendement, aanpasbaar
aantal_slecht = (df_rend["tarief_per_uur"] < ondergrens).sum()

st.markdown(f"### 📌 Extra Inzichten over {titel_tarief}")
col1, col2, col3 = st.columns(3)
# Alleen afronden bij presentatie, niet in berekening!
col1.metric(f"Gemiddeld {titel_tarief}", f"€ {gemiddeld_tarief:.2f}")
col2.metric(f"Mediaan {titel_tarief}", f"€ {mediaan_tarief:.2f}")
col3.metric(f"Aantal bedrijven < €{ondergrens}", f"{aantal_slecht}")

# Horizontale bar chart van tarief per uur per bedrijf
fig = px.bar(
    df_rend,
    x="tarief_per_uur",
    y="companyname",
    orientation="h",
    title=f"{titel_tarief} per bedrijf",
    labels={
        "tarief_per_uur": titel_tarief,
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Werkelijke Opbrengst"
    },
    height=600
)
fig.update_layout(yaxis={'categoryorder': 'total ascending'}, margin={'l': 150})

# --- Volledige tabel: tarief per uur ---
st.markdown(f"### 🧾 Volledige tabel: {titel_tarief}")
bedrijf_zoek = st.text_input("🔎 Zoek op bedrijfsnaam in de tarieftabel")
df_rend_filtered = df_rend.copy()
if bedrijf_zoek:
    df_rend_filtered = df_rend_filtered[df_rend_filtered["companyname"].str.contains(bedrijf_zoek, case=False, na=False)]
# Hernoem de kolom voor presentatie alleen als het een DataFrame is
if isinstance(df_rend_filtered, pd.DataFrame) and "tarief_per_uur" in df_rend_filtered.columns:
    df_rend_filtered = df_rend_filtered.rename(columns={"tarief_per_uur": titel_tarief})
# Selecteer alleen de gewenste kolommen als ze bestaan
kolommen = [col for col in ["companyname", "totaal_uren", "totalpayed", titel_tarief] if col in df_rend_filtered.columns]
df_rend_present = df_rend_filtered[kolommen].copy()
if isinstance(df_rend_present, pd.DataFrame):
    df_rend_present = df_rend_present.rename(columns={
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Opbrengst"
    })
    st.dataframe(df_rend_present.style.format({
        "Totaal Uren": "{:.1f}",
        "Opbrengst": "€ {:.2f}",
        titel_tarief: "€ {:.2f}"
    }), use_container_width=True)

# === VISUELE PLOT VAN RISICOCATEGORIEËN ===
# Removed visual risk analysis section per instructions

# === Analyse: welke bedrijven leveren veel op vs. kosten veel tijd ===
st.markdown(f"### ⏱️ Tijdsbesteding versus Opbrengst per bedrijf")

# Dynamisch droppen op omzet_kolom
df_rend_clean = df_rend.dropna(subset=["tarief_per_uur", "totaal_uren", omzet_kolom])
df_rend_clean = df_rend_clean[df_rend_clean["tarief_per_uur"] > 0]

fig_scatter = px.scatter(
    df_rend_clean,
    x="totaal_uren",
    y=omzet_kolom,
    hover_name="companyname",
    hover_data={titel_tarief: df_rend_clean["tarief_per_uur"], omzet_kolom: True},
    size="tarief_per_uur",
    color="tarief_per_uur",
    color_continuous_scale="Viridis",
    title=f"Tijdsinvestering vs Opbrengst per bedrijf (Hover voor details)",
    labels={
        "totaal_uren": "Totaal Uren",
        omzet_kolom: "Opbrengst",
        "tarief_per_uur": titel_tarief,
        "companyname": "Bedrijf"
    }
)
fig_scatter.update_layout(height=700)
st.plotly_chart(fig_scatter, use_container_width=True)

# Treemap: tijd vs opbrengst per bedrijf
st.markdown(f"### 🌳 Treemap van tijdsinvestering en opbrengst per bedrijf")

fig_treemap = px.treemap(
    df_rend_clean,
    path=["companyname"],
    values="totaal_uren",
    color=omzet_kolom,
    hover_data={titel_tarief: df_rend_clean["tarief_per_uur"], omzet_kolom: True},
    color_continuous_scale="RdYlGn",
    title=f"Treemap: tijdsinvestering (grootte) vs opbrengst (kleur) per bedrijf",
    labels={
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren",
        omzet_kolom: "Opbrengst",
        "tarief_per_uur": titel_tarief
    }
)
st.plotly_chart(fig_treemap, use_container_width=True)


# === Extra inzichten: Percentage tijdsbesteding en ROI-ratio ===

# 1. Percentage tijdsbesteding per bedrijf
totale_uren_all = bedrijfsstats["totaal_uren"].sum()
bedrijfsstats["% tijdsbesteding"] = (bedrijfsstats["totaal_uren"] / totale_uren_all * 100).round(1)

#
#
# 2. Verwachte opbrengst berekenen: kostprijs * amount
# Gebruik df_projectlines (alle regels) voor omzetanalyses
df_projectlines["sellingprice"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce")
df_projectlines["amount"] = pd.to_numeric(df_projectlines["amount"], errors="coerce")
df_projectlines["verwachte_opbrengst"] = df_projectlines["sellingprice"] * df_projectlines["amount"]

verwachte_opbrengst_per_bedrijf = df_projectlines.groupby("bedrijf_id")["verwachte_opbrengst"].sum().reset_index()  # type: ignore
verwachte_opbrengst_per_bedrijf.columns = ["bedrijf_id", "verwachte_opbrengst"]

bedrijfsstats = bedrijfsstats.merge(verwachte_opbrengst_per_bedrijf, on="bedrijf_id", how="left").copy()

 # Verbeterde realisatie-marge berekening
bedrijfsstats["realisatie_marge"] = (
    (bedrijfsstats["totalpayed"] - bedrijfsstats["verwachte_opbrengst"]) / bedrijfsstats["verwachte_opbrengst"]
).round(2)

# Filter: enkel bedrijven met valide verwachte_opbrengst > 0
bedrijfsstats = bedrijfsstats[
    bedrijfsstats["verwachte_opbrengst"] > 0
].copy()

st.markdown("### 🧮 Extra KPI's")

# Topbedrijf op basis van hoogste realisatie-marge
taggr = bedrijfsstats.copy()
taggr["realisatie_marge"] = taggr["realisatie_marge"] if "realisatie_marge" in taggr.columns else None

# Vóór gebruik van .iloc[0] (bijvoorbeeld top_realisatie)
if not bedrijfsstats.empty and "realisatie_marge" in bedrijfsstats.columns:
    sorted_realisatie = pd.DataFrame(bedrijfsstats).sort_values(by="realisatie_marge", ascending=False)
    if not sorted_realisatie.empty:
        top_realisatie = sorted_realisatie.iloc[0]
        st.metric("🏆 Beste realisatie-marge", f"{top_realisatie['realisatie_marge']:.2f}", help=f"{top_realisatie['companyname']}")
    else:
        st.warning("Geen data beschikbaar voor realisatie-marge.")
else:
    st.warning("Geen data beschikbaar voor realisatie-marge.")

# Vóór gebruik van .iloc[0] (bijvoorbeeld top_opbrengst)
if not bedrijfsstats.empty and "totalpayed" in bedrijfsstats.columns:
    sorted_opbrengst = pd.DataFrame(bedrijfsstats).sort_values(by="totalpayed", ascending=False)
    if not sorted_opbrengst.empty:
        top_opbrengst = sorted_opbrengst.iloc[0]
        st.metric("💰 Hoogste opbrengst totaal", f"€ {top_opbrengst['totalpayed']:.2f}", help=f"{top_opbrengst['companyname']}")
    else:
        st.warning("Geen data beschikbaar voor hoogste opbrengst.")
else:
    st.warning("Geen data beschikbaar voor hoogste opbrengst.")

# Vóór gebruik van .iloc[0] (bijvoorbeeld top_tarief)
if not bedrijfsstats.empty and "tarief_per_uur" in bedrijfsstats.columns:
    sorted_tarief = pd.DataFrame(bedrijfsstats).sort_values(by="tarief_per_uur", ascending=False)
    if not sorted_tarief.empty:
        top_tarief = sorted_tarief.iloc[0]
        st.metric(f"⚙️ Hoogste {titel_tarief}", f"€ {top_tarief['tarief_per_uur']:.2f}", help=f"{top_tarief['companyname']}")
    else:
        st.warning(f"Geen data beschikbaar voor hoogste {titel_tarief}.")
else:
    st.warning(f"Geen data beschikbaar voor hoogste {titel_tarief}.")

 # Uitleg over de realisatie-marge in een expander
with st.expander("ℹ️ Wat is de realisatie-marge?"):
    st.markdown("""
    De realisatie-marge vergelijkt het verschil tussen _werkelijke opbrengst_ en _verwachte opbrengst_.  
    - **0.00** betekent dat de opbrengst gelijk is aan verwachting.  
    - Negatief betekent _minder_ opbrengst dan verwacht (verlies of korting).  
    - Positief betekent _meer_ opbrengst dan verwacht (meerkosten of upsell).  
    """)

col1, col2 = st.columns(2)
# Alleen afronden bij presentatie, niet in data!
col1.metric("Totale bestede uren", f"{totale_uren_all:.0f} uur")
realisatie_gem = bedrijfsstats["realisatie_marge"].mean()
col2.metric("Gemiddelde realisatie-marge", f"{realisatie_gem:.2f}")

# Tabel tonen met nieuwe inzichten

st.markdown("### 📋 Bedrijven met % tijdsbesteding en realisatie-marge")
df_extra = bedrijfsstats[["companyname", "totaal_uren", "% tijdsbesteding", "totalpayed", "verwachte_opbrengst", "realisatie_marge"]]
df_extra = df_extra.dropna(subset=["realisatie_marge"]).copy()  # type: ignore
# Alleen afronden bij presentatie, niet in data!
st.dataframe(df_extra.sort_values("realisatie_marge", ascending=True).style.format({  # type: ignore
    "totaal_uren": "{:.1f}",
    "% tijdsbesteding": "{:.1f}",
    "totalpayed": "€ {:.2f}",
    "verwachte_opbrengst": "€ {:.2f}",
    "realisatie_marge": "{:.2f}"
}), use_container_width=True)

 # === Urenverdeling per bedrijf (percentage van totale uren) ===
st.markdown("### ⏳ Urenverdeling per bedrijf")

df_urenverdeling = bedrijfsstats[["companyname", "totaal_uren", "% tijdsbesteding"]].copy()
df_urenverdeling = df_urenverdeling.sort_values("% tijdsbesteding", ascending=False)  # type: ignore

fig_uren = px.bar(
    df_urenverdeling,
    x="% tijdsbesteding",
    y="companyname",
    orientation="h",
    labels={
        "% tijdsbesteding": "Percentage Tijdsbesteding",
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren"
    },
    title="Verdeling van totaal bestede uren per bedrijf",
    height=600
)
fig_uren.update_layout(yaxis={'categoryorder': 'total ascending'}, margin={'l': 150})

st.plotly_chart(fig_uren, use_container_width=True)

# === Pareto-analyse (80/20-regel) ===
st.markdown("### 🧠 Pareto-analyse: Welk aantal bedrijven leveren het meeste op?")

# Dynamische omzetlabel voor Pareto
omzet_label = "Omzet (€)" if omzet_optie == "Geplande omzet (offerte)" else "Werkelijke Omzet (€)"

df_pareto = bedrijfsstats[["companyname", omzet_kolom]].copy()
df_pareto = df_pareto.sort_values(omzet_kolom, ascending=False).reset_index(drop=True)  # type: ignore
df_pareto["cumulatieve_opbrengst"] = df_pareto[omzet_kolom].cumsum()
totale_opbrengst = df_pareto[omzet_kolom].sum()
df_pareto["cumulatief_percentage"] = (df_pareto["cumulatieve_opbrengst"] / totale_opbrengst * 100).round(2)

fig_pareto = px.line(
    df_pareto,
    x=df_pareto.index + 1,
    y="cumulatief_percentage",
    markers=True,
    labels={
        "x": "Aantal Bedrijven",
        "cumulatief_percentage": "Cumulatieve Opbrengst (%)",
        "companyname": "Bedrijf",
        omzet_kolom: omzet_label
    },
    title=f"Pareto-analyse: cumulatieve opbrengst over bedrijven ({omzet_label})"
)
fig_pareto.add_hline(y=80, line_dash="dash", line_color="red")
fig_pareto.update_layout(xaxis_title="Top X bedrijven", yaxis_title="Cumulatieve % van totale opbrengst", height=500)

st.plotly_chart(fig_pareto, use_container_width=True)

# Tekstuele conclusie
bedrijven_80pct = (df_pareto["cumulatief_percentage"] <= 80).sum()
st.info(f"💡 {bedrijven_80pct} bedrijven zijn samen goed voor 80% van de totale opbrengst.")


# === AI-INSIGHTS & AUTOMATISCHE AANBEVELINGEN ===
st.markdown("### 🤖 AI-Inzichten & Automatische Aanbevelingen")

#
# Topklanten voor intensivering (geen wijziging nodig)
top_klanten = df_rend[(df_rend["tarief_per_uur"] > mediaan_tarief) & (df_rend["totaal_uren"] < 50)].sort_values("tarief_per_uur", ascending=False).head(5)  # type: ignore



st.markdown("#### 3. Clustering van bedrijven op basis van prestaties")
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

cluster_data = bedrijfsstats[["totaal_uren", "totalpayed", "tarief_per_uur"]].dropna()  # type: ignore
if len(cluster_data) < 4:
    st.warning(f"❌ Niet genoeg bedrijven ({len(cluster_data)}) om clustering uit te voeren (minimaal 4 vereist). Clustering wordt overgeslagen.")
else:
    scaled = StandardScaler().fit_transform(cluster_data)
    kmeans = KMeans(n_clusters=4, random_state=42).fit(scaled)
    cluster_data["cluster"] = kmeans.labels_
    fig_cluster = px.scatter_3d(
        cluster_data,
        x="totaal_uren", y="totalpayed", z="tarief_per_uur",
        color="cluster",
        title="3D Clustering van Bedrijven",
        hover_name=bedrijfsstats.loc[cluster_data.index, "companyname"],
        labels={
            "totaal_uren": "Totaal Uren",
            "totalpayed": "Werkelijke Opbrengst",
            "tarief_per_uur": titel_tarief,
            "cluster": "Cluster"
        }
    )
    st.plotly_chart(fig_cluster, use_container_width=True)

 
#
# === AI-adviseur: automatisch gegenereerd advies per bedrijf op basis van prestaties ===
st.markdown("### 🧠 AI-adviseur: Automatisch gegenereerd advies per bedrijf")

bedrijf_advies = st.selectbox("📌 Kies een bedrijf voor advies", bedrijfsstats["companyname"].dropna().unique())  # type: ignore

# Vóór gebruik van .iloc[0] bij bedrijf_data en bedrijf_info
bedrijf_advies_namen_lijst = bedrijfsstats["companyname"].tolist() if not bedrijfsstats.empty and "companyname" in bedrijfsstats.columns else []
if not bedrijfsstats.empty and bedrijf_advies in bedrijf_advies_namen_lijst:
    bedrijf_info = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_advies].iloc[0]  # type: ignore
else:
    st.warning("Geen data beschikbaar voor geselecteerd bedrijf voor advies.")
    bedrijf_info = None

# Advies prompt veilig opbouwen
if bedrijf_info is not None:
    totaal_uren_str = f"{bedrijf_info['totaal_uren']:.1f}" if 'totaal_uren' in bedrijf_info else '-'
    werkelijke_opbrengst_str = f"€{bedrijf_info['totalpayed']:.2f}" if 'totalpayed' in bedrijf_info else '-'
    verwachte_opbrengst_str = f"€{bedrijf_info['verwachte_opbrengst']:.2f}" if 'verwachte_opbrengst' in bedrijf_info else '-'
    realisatie_marge_str = f"{bedrijf_info['realisatie_marge']:.2f}" if 'realisatie_marge' in bedrijf_info else '-'
    tarief_per_uur_str = f"€{bedrijf_info['tarief_per_uur']:.2f}" if 'tarief_per_uur' in bedrijf_info else '-'
    tijdsbesteding_str = f"{bedrijf_info['% tijdsbesteding']:.1f}%" if '% tijdsbesteding' in bedrijf_info else '-'
    gemiddeld_tarief_str = f"€{bedrijf_info['gemiddeld_tarief']:.2f}" if 'gemiddeld_tarief' in bedrijf_info and pd.notnull(bedrijf_info['gemiddeld_tarief']) else '-'
else:
    totaal_uren_str = werkelijke_opbrengst_str = verwachte_opbrengst_str = realisatie_marge_str = tarief_per_uur_str = tijdsbesteding_str = gemiddeld_tarief_str = '-'

advies_prompt = f"""
Je bent een zakelijke AI-consultant. Geef beknopt maar concreet advies voor het volgende bedrijf:
- Naam: {bedrijf_advies}
- Totaal bestede uren: {totaal_uren_str}
- Werkelijke opbrengst: {werkelijke_opbrengst_str}
- Verwachte opbrengst: {verwachte_opbrengst_str}
- Gemiddeld tarief per uur (op basis van alle taken): {gemiddeld_tarief_str}
- Uurtarief (gebaseerd op keuze omzet): {tarief_per_uur_str}
- realisatie-marge: {realisatie_marge_str}
- % tijdsbesteding: {tijdsbesteding_str}

Geef suggesties over klantprioriteit, verbeterpotentieel, tariefoptimalisatie of workloadplanning. Houd het zakelijk en feitelijk.
"""

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def genereer_advies(prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Je bent een zakelijke AI-consultant die kort, feitelijk en strategisch advies geeft op basis van inputdata."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=500
        )
        content = response.choices[0].message.content
        if content is not None:
            return content.strip()
        else:
            return "⚠️ Geen AI-advies ontvangen."
    except Exception as e:
        return f"⚠️ Fout bij ophalen van AI-advies: {e}"

# Only generate and display AI advice when the button is pressed
if st.button("Genereer AI-advies"):
    advies_output = genereer_advies(advies_prompt)
    st.info(advies_output)

st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2025
</div>
""", unsafe_allow_html=True)
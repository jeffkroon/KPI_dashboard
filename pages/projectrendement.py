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

# --- LOAD DATA ---
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")

# --- DATA EXACT ZOALS IN app.py ---
df_projects_raw = load_data_df("projects", columns=["id", "company_id", "archived", "totalexclvat"])
if not isinstance(df_projects_raw, pd.DataFrame):
    df_projects_raw = pd.concat(list(df_projects_raw), ignore_index=True)
df_companies = load_data_df("companies", columns=["id", "companyname"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)
df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
if not isinstance(df_employees, pd.DataFrame):
    df_employees = pd.concat(list(df_employees), ignore_index=True)
df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "amountwritten", "sellingprice", "amount"])
if not isinstance(df_projectlines, pd.DataFrame):
    df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "subject"])
if not isinstance(df_invoices, pd.DataFrame):
    df_invoices = pd.concat(list(df_invoices), ignore_index=True)

# Kolomhernoemingen en numerieke conversies
if 'bedrijf_id' not in df_projectlines.columns and 'bedrijf_id' in df_projectlines.columns:
    df_projectlines = df_projectlines.rename(columns={'bedrijf_id': 'bedrijf_id'})
if 'companyname' not in df_companies.columns and 'bedrijf_naam' in df_companies.columns:
    df_companies = df_companies.rename(columns={'bedrijf_naam': 'companyname'})
for col in ["amountwritten", "sellingprice"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Bereken totaal uren per bedrijf direct in SQL
uren_per_bedrijf = load_data_df("projectlines_per_company", columns=["bedrijf_id", "SUM(CAST(amountwritten AS FLOAT)) as totaal_uren"]).groupby("bedrijf_id").sum().reset_index()
uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Bereken totaal gefactureerd per bedrijf direct in SQL
factuurbedrag_per_bedrijf = load_data_df("invoices", columns=["company_id", "SUM(CAST(totalpayed AS FLOAT)) as totalpayed"], where="fase = 'Factuur'").groupby("company_id").sum().reset_index()
factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf.rename(columns={"company_id": "bedrijf_id"})

# Combineer stats per bedrijf
bedrijfsstats = uren_per_bedrijf.merge(factuurbedrag_per_bedrijf, on="bedrijf_id", how="outer")
bedrijfsstats = bedrijfsstats.merge(df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left")
bedrijfsstats = bedrijfsstats.drop(columns=[col for col in ['id'] if col in bedrijfsstats.columns])
bedrijfsstats["totaal_uren"] = bedrijfsstats["totaal_uren"].fillna(0)
bedrijfsstats["totalpayed"] = bedrijfsstats["totalpayed"].fillna(0)
bedrijfsstats["werkelijk_tarief_per_uur"] = bedrijfsstats["totalpayed"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)

# Nu kun je bedrijfsstats gebruiken voor verdere analyses en visualisaties, net als in app.py
# De rest van de analyses en visualisaties kun je nu baseren op bedrijfsstats
# ... bestaande analyses en visualisaties ...

# Filter bedrijven met daadwerkelijk gewerkte uren
bedrijfsstats = bedrijfsstats[bedrijfsstats["totaal_uren"] > 0].copy()

# Bereken rendement per uur per bedrijf
bedrijfsstats["rendement_per_uur"] = (
    bedrijfsstats["totalpayed"] / bedrijfsstats["totaal_uren"]
).round(2)


# Voeg werkelijke omzet toe aan projects via een merge
df_projects_raw = df_projects_raw[df_projects_raw["archived"] != True].copy()  # type: ignore
df_projects_raw["totalexclvat"] = pd.to_numeric(df_projects_raw["totalexclvat"], errors="coerce")
df_projects_raw = df_projects_raw.merge(bedrijfsstats, left_on="company_id", right_on="bedrijf_id", how="left")
df_projects_raw["werkelijke_opbrengst"] = df_projects_raw["totalpayed"].fillna(0)
df_projects_raw["totaal_uren"] = df_projects_raw["totaal_uren"].fillna(0)


# Sorteer en filter op rendement per uur, hoogste eerst, filter 0 en NaN
df_rend = bedrijfsstats.copy()
df_rend = df_rend.dropna(subset=["rendement_per_uur"]).copy()  # type: ignore
df_rend = df_rend[df_rend["rendement_per_uur"] > 0].copy()  # type: ignore
df_rend = df_rend.sort_values("rendement_per_uur", ascending=False).copy()  # type: ignore

# === RISICOCATEGORIE TOEVOEGEN OP BASIS VAN RENDEMENT EN TIJDSBESTEDING ===
# Removed categoriseer_risico function and its apply call per instructions

# KPI-cards: top 3 bedrijven met hoogste rendement per uur
st.markdown("### 🥇 Top 3 bedrijven op basis van rendement per uur")
cols = st.columns(3)
# Let op: afronden alleen bij presentatie, niet in berekening!
for i, (_, row) in enumerate(df_rend.head(3).iterrows()):
    cols[i].metric(
        label=f"{row['companyname']}",
        value=f"€ {row['rendement_per_uur']:.2f}/uur"
    )

# KPI-cards: bottom 10 bedrijven met laagste rendement per uur
st.markdown("### 🛑 Bottom 10 bedrijven op basis van rendement per uur")
df_bottom_10 = df_rend.nsmallest(10, "rendement_per_uur")[["companyname", "totaal_uren", "totalpayed", "rendement_per_uur"]]
df_bottom_10.columns = ["Bedrijf", "Totaal Uren", "Opbrengst", "Rendement per Uur"]
# Alleen afronden in presentatie, niet in data!
st.dataframe(df_bottom_10.style.format({
    "Totaal Uren": "{:.1f}",
    "Opbrengst": "€ {:.2f}",
    "Rendement per Uur": "€ {:.2f}"
}), use_container_width=True)

# Extra inzichten: gemiddeld rendement, mediaan, en aantal bedrijven onder drempel
gemiddeld_rendement = df_rend["rendement_per_uur"].mean()
mediaan_rendement = df_rend["rendement_per_uur"].median()
ondergrens = 75  # drempelrendement, aanpasbaar
aantal_slecht = (df_rend["rendement_per_uur"] < ondergrens).sum()

st.markdown("### 📌 Extra Inzichten over Bedrijfsrendement")
col1, col2, col3 = st.columns(3)
# Alleen afronden bij presentatie, niet in berekening!
col1.metric("Gemiddeld rendement per uur", f"€ {gemiddeld_rendement:.2f}")
col2.metric("Mediaan rendement per uur", f"€ {mediaan_rendement:.2f}")
col3.metric(f"Aantal bedrijven < €{ondergrens}", f"{aantal_slecht}")

# Horizontale bar chart van rendement per uur per bedrijf
fig = px.bar(
    df_rend,
    x="rendement_per_uur",
    y="companyname",
    orientation="h",
    title="Rendement per uur per bedrijf",
    labels={
        "rendement_per_uur": "Rendement per Uur",
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Werkelijke Opbrengst"
    },
    height=600
)
fig.update_layout(yaxis={'categoryorder': 'total ascending'}, margin={'l': 150})

st.markdown("### 🧾 Volledige rendementstabel")
# Alleen afronden bij presentatie, niet in data!
st.dataframe(df_rend.style.format({
    "totaal_uren": "{:.1f}",
    "totalpayed": "€ {:.2f}",
    "rendement_per_uur": "€ {:.2f}"
}), use_container_width=True)

# === VISUELE PLOT VAN RISICOCATEGORIEËN ===
# Removed visual risk analysis section per instructions

# === Analyse: welke bedrijven leveren veel op vs. kosten veel tijd ===
st.markdown("### ⏱️ Tijdsbesteding versus Opbrengst per bedrijf")

df_rend_clean = df_rend.dropna(subset=["rendement_per_uur", "totaal_uren", "totalpayed"])
df_rend_clean = df_rend_clean[df_rend_clean["rendement_per_uur"] > 0]

fig_scatter = px.scatter(
    df_rend_clean,
    x="totaal_uren",
    y="totalpayed",
    hover_name="companyname",
    hover_data={"rendement_per_uur": True},
    size="rendement_per_uur",
    color="rendement_per_uur",
    color_continuous_scale="Viridis",
    title="Tijdsinvestering vs Opbrengst per bedrijf (Hover voor details)",
    labels={
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Werkelijke Opbrengst",
        "rendement_per_uur": "Rendement per Uur",
        "companyname": "Bedrijf"
    }
)
fig_scatter.update_layout(height=700)
st.plotly_chart(fig_scatter, use_container_width=True)

# Treemap: tijd vs opbrengst per bedrijf
st.markdown("### 🌳 Treemap van tijdsinvestering en opbrengst per bedrijf")

fig_treemap = px.treemap(
    df_rend_clean,
    path=["companyname"],
    values="totaal_uren",
    color="totalpayed",
    hover_data={"rendement_per_uur": True},
    color_continuous_scale="RdYlGn",
    title="Treemap: tijdsinvestering (grootte) vs opbrengst (kleur) per bedrijf",
    labels={
        "companyname": "Bedrijf",
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Werkelijke Opbrengst",
        "rendement_per_uur": "Rendement per Uur"
    }
)
st.plotly_chart(fig_treemap, use_container_width=True)


# === Extra inzichten: Percentage tijdsbesteding en ROI-ratio ===

# 1. Percentage tijdsbesteding per bedrijf
totale_uren_all = bedrijfsstats["totaal_uren"].sum()
bedrijfsstats["% tijdsbesteding"] = (bedrijfsstats["totaal_uren"] / totale_uren_all * 100).round(1)

# 2. Verwachte opbrengst berekenen: kostprijs * amount
df_projectlines["sellingprice"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce")
df_projectlines["amount"] = pd.to_numeric(df_projectlines["amount"], errors="coerce")
df_projectlines["verwachte_opbrengst"] = df_projectlines["sellingprice"] * df_projectlines["amount"]

verwachte_opbrengst_per_bedrijf = df_projectlines.groupby("bedrijf_id")["verwachte_opbrengst"].sum().reset_index()  # type: ignore
verwachte_opbrengst_per_bedrijf.columns = ["bedrijf_id", "verwachte_opbrengst"]

bedrijfsstats = bedrijfsstats.merge(verwachte_opbrengst_per_bedrijf, on="bedrijf_id", how="left").copy()

bedrijfsstats["realisatie_ratio"] = (
    bedrijfsstats["totalpayed"] / bedrijfsstats["verwachte_opbrengst"]
).round(2)

# Filter: verwijder rijen waarbij realisatie_ratio NaN is, verwachte_opbrengst 0 of leeg, of realisatie_ratio gelijk aan 0
bedrijfsstats = bedrijfsstats[
    bedrijfsstats["realisatie_ratio"].notna() &
    (bedrijfsstats["verwachte_opbrengst"] > 0) &
    (bedrijfsstats["realisatie_ratio"] > 0)
].copy()

st.markdown("### 🧮 Extra KPI's")

# Topbedrijf op basis van hoogste realisatie-ratio
taggr = bedrijfsstats.copy()
taggr["realisatie_ratio"] = taggr["totalpayed"] / taggr["verwachte_opbrengst"] if "verwachte_opbrengst" in taggr.columns and "totalpayed" in taggr.columns else None
st.write("bedrijfsstats na berekening realisatie_ratio:", taggr.head())

# Vóór gebruik van .iloc[0] (bijvoorbeeld top_realisatie)
if not bedrijfsstats.empty and "realisatie_ratio" in bedrijfsstats.columns:
    sorted_realisatie = pd.DataFrame(bedrijfsstats).sort_values(by="realisatie_ratio", ascending=False)
    if not sorted_realisatie.empty:
        top_realisatie = sorted_realisatie.iloc[0]
        st.metric("🏆 Beste realisatie-ratio", f"{top_realisatie['realisatie_ratio']:.2f}", help=f"{top_realisatie['companyname']}")
    else:
        st.warning("Geen data beschikbaar voor realisatie-ratio.")
else:
    st.warning("Geen data beschikbaar voor realisatie-ratio.")

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

# Vóór gebruik van .iloc[0] (bijvoorbeeld top_rendement)
if not bedrijfsstats.empty and "rendement_per_uur" in bedrijfsstats.columns:
    sorted_rendement = pd.DataFrame(bedrijfsstats).sort_values(by="rendement_per_uur", ascending=False)
    if not sorted_rendement.empty:
        top_rendement = sorted_rendement.iloc[0]
        st.metric("⚙️ Hoogste rendement per uur", f"€ {top_rendement['rendement_per_uur']:.2f}", help=f"{top_rendement['companyname']}")
    else:
        st.warning("Geen data beschikbaar voor hoogste rendement per uur.")
else:
    st.warning("Geen data beschikbaar voor hoogste rendement per uur.")

# Uitleg over de realisatie-ratio in een expander
with st.expander("ℹ️ Wat is de realisatie-ratio?"):
    st.markdown("""
    De realisatie-ratio vergelijkt de _werkelijke opbrengst_ van een bedrijf met de _verwachte opbrengst_ (gebaseerd op geoffreerde tarieven en hoeveelheden).  
    - Een ratio van **1.0** betekent dat het project exact volgens verwachting is uitgevoerd.  
    - Lager dan 1.0 betekent dat er minder opbrengst is gerealiseerd dan verwacht (bijvoorbeeld door korting, minder uren geschreven of verlies).  
    - Hoger dan 1.0 betekent dat er meer is verdiend dan vooraf begroot (bijvoorbeeld door extra werk of hogere tarieven).
    """)

col1, col2 = st.columns(2)
# Alleen afronden bij presentatie, niet in data!
col1.metric("Totale bestede uren", f"{totale_uren_all:.0f} uur")
realisatie_gem = bedrijfsstats["realisatie_ratio"].mean()
col2.metric("Gemiddelde realisatie-ratio", f"{realisatie_gem:.2f}")

# Tabel tonen met nieuwe inzichten

st.markdown("### 📋 Bedrijven met % tijdsbesteding en realisatie-ratio")
df_extra = bedrijfsstats[["companyname", "totaal_uren", "% tijdsbesteding", "totalpayed", "verwachte_opbrengst", "realisatie_ratio"]]
df_extra = df_extra.dropna(subset=["realisatie_ratio"]).copy()  # type: ignore
# Alleen afronden bij presentatie, niet in data!
st.dataframe(df_extra.sort_values("realisatie_ratio", ascending=True).style.format({  # type: ignore
    "totaal_uren": "{:.1f}",
    "% tijdsbesteding": "{:.1f}",
    "totalpayed": "€ {:.2f}",
    "verwachte_opbrengst": "€ {:.2f}",
    "realisatie_ratio": "{:.2f}"
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

df_pareto = bedrijfsstats[["companyname", "totalpayed"]].copy()
df_pareto = df_pareto.sort_values("totalpayed", ascending=False).reset_index(drop=True)  # type: ignore
df_pareto["cumulatieve_opbrengst"] = df_pareto["totalpayed"].cumsum()
totale_opbrengst = df_pareto["totalpayed"].sum()
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
        "totalpayed": "Werkelijke Opbrengst"
    },
    title="Pareto-analyse: cumulatieve opbrengst over bedrijven"
)
fig_pareto.add_hline(y=80, line_dash="dash", line_color="red")
fig_pareto.update_layout(xaxis_title="Top X bedrijven", yaxis_title="Cumulatieve % van totale opbrengst", height=500)

st.plotly_chart(fig_pareto, use_container_width=True)

# Tekstuele conclusie
bedrijven_80pct = (df_pareto["cumulatief_percentage"] <= 80).sum()
st.info(f"💡 {bedrijven_80pct} bedrijven zijn samen goed voor 80% van de totale opbrengst.")


# === AI-INSIGHTS & AUTOMATISCHE AANBEVELINGEN ===
st.markdown("### 🤖 AI-Inzichten & Automatische Aanbevelingen")

# Topklanten voor intensivering
top_klanten = df_rend[(df_rend["rendement_per_uur"] > mediaan_rendement) & (df_rend["totaal_uren"] < 50)].sort_values("rendement_per_uur", ascending=False).head(5)  # type: ignore


# === 🔮 AI Forecasting: Verwacht KPI's via regressiemodel ===

st.markdown("### 🔮 AI Simulatie per Bedrijf – Wat als Scenario's & Aanbevelingen")

# Voorbereiding data voor regressie
reg_data = bedrijfsstats[["totaal_uren", "verwachte_opbrengst", "totalpayed", "realisatie_ratio", "rendement_per_uur"]].dropna()  # type: ignore

X = reg_data[["totaal_uren", "verwachte_opbrengst"]]
y = reg_data["realisatie_ratio"]

model = RandomForestRegressor(random_state=42)
model.fit(X, y)

# Feature importance berekenen
feature_importances = model.feature_importances_
importances_df = pd.DataFrame({
    'Feature': X.columns,  # type: ignore
    'Importance': feature_importances
}).sort_values(by='Importance', ascending=False)

bedrijven_keuze = bedrijfsstats["companyname"].dropna().unique()  # type: ignore
bedrijf_selectie = st.selectbox("Selecteer een bedrijf", bedrijven_keuze)

# Vóór gebruik van .iloc[0] bij bedrijf_data en bedrijf_info
bedrijf_namen_lijst = bedrijfsstats["companyname"].tolist() if not bedrijfsstats.empty and "companyname" in bedrijfsstats.columns else []
if not bedrijfsstats.empty and bedrijf_selectie in bedrijf_namen_lijst:
    bedrijf_data = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_selectie].iloc[0]  # type: ignore
else:
    st.warning("Geen data beschikbaar voor geselecteerd bedrijf.")
    bedrijf_data = None

default_uren = int(bedrijf_data["totaal_uren"]) if bedrijf_data is not None and "totaal_uren" in bedrijf_data else 1
default_opbrengst = int(bedrijf_data["verwachte_opbrengst"]) if bedrijf_data is not None and "verwachte_opbrengst" in bedrijf_data else 1

# Alleen afronden bij presentatie, niet in data!
sim_uren = st.number_input("⚙️ Stel totaal bestede uren in", min_value=1, value=default_uren)
sim_opbrengst = st.number_input("💰 Stel verwachte opbrengst in (€)", min_value=1, value=default_opbrengst)

X_sim = pd.DataFrame([[sim_uren, sim_opbrengst]], columns=['totaal_uren', 'verwachte_opbrengst'])  # type: ignore
sim_roi = model.predict(X_sim)[0]

st.metric("📈 Voorspelde realisatie-ratio", f"{sim_roi:.2f}")
if sim_roi < 1.0:
    st.warning("⚠️ Verwachte realisatie-ratio lager dan 1.0 – verlieslatend scenario.")
elif sim_roi < 1.2:
    st.info("ℹ️ Realisatie-ratio is marginaal – overweeg tariefverhoging of urenverlaging.")
else:
    st.success("✅ Verwachte realisatie-ratio is goed – rendabel project.")

# Suggestie bij lage realisatie-ratio
if sim_roi < 1.2:
    st.markdown("### 📌 AI-advies:")
    ratio_verbeter = 1.5
    nodig_opbrengst = ratio_verbeter * sim_uren
    extra_opbrengst = nodig_opbrengst - sim_opbrengst
    procent_tariefstijging = (extra_opbrengst / sim_opbrengst * 100)
    st.write(f"📊 Om een realisatie-ratio van 1.5 te behalen, zou je de opbrengst moeten verhogen met ~€{extra_opbrengst:.0f} → dat is een tariefstijging van {procent_tariefstijging:.1f}%.")

# Elasticiteitsgrafiek
st.markdown("### 📉 Elasticiteitsgrafiek: hoe verandert realisatie-ratio bij toenemende uren")

uren_range = np.arange(10, 200, 10)
opbrengst = sim_opbrengst  # opbrengst constant
realisatie_pred = model.predict(
    pd.DataFrame(
        np.column_stack((uren_range, [opbrengst]*len(uren_range))),
        columns=['totaal_uren', 'verwachte_opbrengst']  # type: ignore
    )
)

fig_elastic = px.line(
    x=uren_range,
    y=realisatie_pred,
    labels={
        "x": "Totaal Uren",
        "y": "Voorspelde realisatie-ratio"
    },
    title="Elasticiteit van realisatie-ratio bij variërende uren (opbrengst constant)"
)
fig_elastic.add_hline(y=1.0, line_dash="dot", line_color="red")
fig_elastic.add_hline(y=1.5, line_dash="dot", line_color="green")
st.plotly_chart(fig_elastic, use_container_width=True)

# === Feature Importance visualisatie ===
st.markdown("### 🔍 Belangrijkste factoren die realisatie-ratio beïnvloeden (Feature Importance)")

if importances_df is not None:
    fig_importance = px.bar(
        importances_df,
        x='Importance',
        y='Feature',
        orientation='h',
        title='Belang van input-variabelen voor realisatie-ratio voorspelling',
        labels={
            "Importance": "Belang",
            "Feature": "Kenmerk"
        }
    )
    fig_importance.update_layout(margin={'l': 150})
    st.plotly_chart(fig_importance, use_container_width=True)


# Pricing engine
st.markdown("### 🧮 Scenario-analyse: Minimale opbrengst voor gewenste realisatie-ratio")
desired_realisatie = st.slider("🎯 Gewenste realisatie-ratio (doelstelling)", min_value=1.0, max_value=3.0, step=0.1, value=1.5)

benodigde_opbrengst = sim_uren * desired_realisatie
st.caption("📝 Je huidige verwachte opbrengst is gebaseerd op je eigen invoer hierboven.")
huidige_opbrengst = sim_opbrengst

st.write(f"�� Om een realisatie-ratio van {desired_realisatie:.1f} te halen bij {sim_uren} uur, is minimaal €{benodigde_opbrengst:.0f} aan opbrengst nodig.")
if sim_uren > 0 and desired_realisatie > 0:
    stijging_pct = (benodigde_opbrengst - huidige_opbrengst) / huidige_opbrengst * 100 if huidige_opbrengst > 0 else float('inf')

    if huidige_opbrengst <= 0:
        st.info("ℹ️ Opbrengst is €0, geen vergelijking mogelijk voor stijging.")
    elif huidige_opbrengst >= benodigde_opbrengst:
        if stijging_pct > 50:
            st.warning(f"⚠️ Je voldoet wel aan de doelratio, maar de opbrengst ligt >50% boven de huidige → mogelijk onrealistisch scenario.")
    else:
        verschil = benodigde_opbrengst - huidige_opbrengst
        st.warning(f"🚀 Je zou je opbrengst moeten verhogen met €{verschil:.0f} (+{stijging_pct:.1f}%) om de doel-ratio te halen.")
    # Historische ratiovergelijking veilig maken
    historisch_ratio = None
    if bedrijf_data is not None and "realisatie_ratio" in bedrijf_data:
        historisch_ratio = bedrijf_data["realisatie_ratio"]
    if historisch_ratio and desired_realisatie > historisch_ratio * 1.5:
        st.info(f"📉 De gewenste ratio ({desired_realisatie:.1f}) ligt fors boven de historische realisatie-ratio van {historisch_ratio:.2f}. Overweeg realistischere planning.")

    # Check op lage inputwaarden
    if sim_uren < 10 or sim_opbrengst < 500:
        st.warning("⚠️ Te lage inputwaarden – oordeel over haalbaarheid is beperkt betrouwbaar.")
else:
    st.info("ℹ️ Voer geldige inputwaarden in om een correcte berekening te maken.")



st.markdown("#### 3. Clustering van bedrijven op basis van prestaties")
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

cluster_data = bedrijfsstats[["totaal_uren", "totalpayed", "rendement_per_uur"]].dropna()  # type: ignore
scaled = StandardScaler().fit_transform(cluster_data)
kmeans = KMeans(n_clusters=4, random_state=42).fit(scaled)
cluster_data["cluster"] = kmeans.labels_
fig_cluster = px.scatter_3d(
    cluster_data,
    x="totaal_uren", y="totalpayed", z="rendement_per_uur",
    color="cluster",
    title="3D Clustering van Bedrijven",
    hover_name=bedrijfsstats.loc[cluster_data.index, "companyname"],
    labels={
        "totaal_uren": "Totaal Uren",
        "totalpayed": "Werkelijke Opbrengst",
        "rendement_per_uur": "Rendement per Uur",
        "cluster": "Cluster"
    }
)
st.plotly_chart(fig_cluster, use_container_width=True)

 
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
    realisatie_ratio_str = f"{bedrijf_info['realisatie_ratio']:.2f}" if 'realisatie_ratio' in bedrijf_info else '-'
    rendement_per_uur_str = f"€{bedrijf_info['rendement_per_uur']:.2f}" if 'rendement_per_uur' in bedrijf_info else '-'
    tijdsbesteding_str = f"{bedrijf_info['% tijdsbesteding']:.1f}%" if '% tijdsbesteding' in bedrijf_info else '-'
else:
    totaal_uren_str = werkelijke_opbrengst_str = verwachte_opbrengst_str = realisatie_ratio_str = rendement_per_uur_str = tijdsbesteding_str = '-'

advies_prompt = f"""
Je bent een zakelijke AI-consultant. Geef beknopt maar concreet advies voor het volgende bedrijf:
- Naam: {bedrijf_advies}
- Totaal bestede uren: {totaal_uren_str}
- Werkelijke opbrengst: {werkelijke_opbrengst_str}
- Verwachte opbrengst: {verwachte_opbrengst_str}
- realisatie-ratio: {realisatie_ratio_str}
- Rendement per uur: {rendement_per_uur_str}
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
    Dunion Dashboard © 2024
</div>
""", unsafe_allow_html=True)
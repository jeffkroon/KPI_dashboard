import streamlit as st
import pandas as pd
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
load_dotenv()
importances_df = None

st.logo("images/dunion-logo-def_donker-06.png")
st.set_page_config(
    page_title="Customer-analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("projectrendement")

# --- LOAD DATA ---
@st.cache_data
def load_data(table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, con=engine)

from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)
df_projects = load_data("projects")
df_projectlines = load_data("projectlines_per_company")
bedrijf_namen = df_projectlines[["bedrijf_id", "bedrijf_naam"]].drop_duplicates()
df_uren = load_data("urenregistratie")
df_projects = df_projects[df_projects["archived"] != True]
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_employees = load_data("employees")
df_companies = load_data("companies")
df_uren = load_data("urenregistratie")
df_projectlines = load_data("projectlines_per_company")
active_project_ids = df_projects["id"].tolist()
df_projectlines = df_projectlines[df_projectlines["offerprojectbase_id"].isin(active_project_ids)]
df_projectlines = df_projectlines[df_projectlines["rowtype_searchname"] == "NORMAAL"]
df_projectlines["amountwritten"] = pd.to_numeric(df_projectlines["amountwritten"], errors="coerce")
df_projectlines["werkelijke_opbrengst"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce") * df_projectlines["amountwritten"]
aggregatie_per_bedrijf = df_projectlines.groupby("bedrijf_id").agg({
    "werkelijke_opbrengst": "sum",
    "amountwritten": "sum"
}).reset_index()
aggregatie_per_bedrijf.columns = ["bedrijf_id", "werkelijke_opbrengst", "totaal_uren"]

aggregatie_per_bedrijf = aggregatie_per_bedrijf.merge(bedrijf_namen, on="bedrijf_id", how="left")

# Bereken rendement per uur per bedrijf
aggregatie_per_bedrijf["rendement_per_uur"] = (
    aggregatie_per_bedrijf["werkelijke_opbrengst"] / aggregatie_per_bedrijf["totaal_uren"]
).round(2)


# Voeg werkelijke omzet toe aan projects via een merge
df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="company_id", right_on="bedrijf_id", how="left")
df_projects["werkelijke_opbrengst"] = df_projects["werkelijke_opbrengst"].fillna(0)
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0)


# Sorteer en filter op rendement per uur, hoogste eerst, filter 0 en NaN
df_rend = aggregatie_per_bedrijf.copy()
df_rend = df_rend.dropna(subset=["rendement_per_uur"])
df_rend = df_rend[df_rend["rendement_per_uur"] > 0]
df_rend = df_rend.sort_values("rendement_per_uur", ascending=False)

# === RISICOCATEGORIE TOEVOEGEN OP BASIS VAN RENDEMENT EN TIJDSBESTEDING ===
# Removed categoriseer_risico function and its apply call per instructions

# KPI-cards: top 3 bedrijven met hoogste rendement per uur
st.markdown("### 🥇 Top 3 bedrijven op basis van rendement per uur")
cols = st.columns(3)
# Let op: afronden alleen bij presentatie, niet in berekening!
for i, (_, row) in enumerate(df_rend.head(3).iterrows()):
    cols[i].metric(
        label=f"{row['bedrijf_naam']}",
        value=f"€ {row['rendement_per_uur']:.2f}/uur"
    )

# KPI-cards: bottom 10 bedrijven met laagste rendement per uur
st.markdown("### 🛑 Bottom 10 bedrijven op basis van rendement per uur")
df_bottom_10 = df_rend.nsmallest(10, "rendement_per_uur")[["bedrijf_naam", "totaal_uren", "werkelijke_opbrengst", "rendement_per_uur"]]
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
    y="bedrijf_naam",
    orientation="h",
    title="Rendement per uur per bedrijf",
    labels={"rendement_per_uur": "€ per uur", "bedrijf_naam": "Bedrijf"},
    height=600
)
fig.update_layout(yaxis={'categoryorder': 'total ascending'}, margin={'l': 150})

st.markdown("### 🧾 Volledige rendementstabel")
# Alleen afronden bij presentatie, niet in data!
st.dataframe(df_rend.style.format({
    "totaal_uren": "{:.1f}",
    "werkelijke_opbrengst": "€ {:.2f}",
    "rendement_per_uur": "€ {:.2f}"
}), use_container_width=True)

# === VISUELE PLOT VAN RISICOCATEGORIEËN ===
# Removed visual risk analysis section per instructions

# === Analyse: welke bedrijven leveren veel op vs. kosten veel tijd ===
st.markdown("### ⏱️ Tijdsbesteding versus Opbrengst per bedrijf")

df_rend_clean = df_rend.dropna(subset=["rendement_per_uur", "totaal_uren", "werkelijke_opbrengst"])
df_rend_clean = df_rend_clean[df_rend_clean["rendement_per_uur"] > 0]

fig_scatter = px.scatter(
    df_rend_clean,
    x="totaal_uren",
    y="werkelijke_opbrengst",
    hover_name="bedrijf_naam",
    hover_data={"rendement_per_uur": True},
    size="rendement_per_uur",
    color="rendement_per_uur",
    color_continuous_scale="Viridis",
    title="Tijdsinvestering vs Opbrengst per bedrijf (Hover voor details)"
)
fig_scatter.update_layout(height=700)
st.plotly_chart(fig_scatter, use_container_width=True)

# Treemap: tijd vs opbrengst per bedrijf
st.markdown("### 🌳 Treemap van tijdsinvestering en opbrengst per bedrijf")

fig_treemap = px.treemap(
    df_rend_clean,
    path=["bedrijf_naam"],
    values="totaal_uren",
    color="werkelijke_opbrengst",
    hover_data={"rendement_per_uur": True},
    color_continuous_scale="RdYlGn",
    title="Treemap: tijdsinvestering (grootte) vs opbrengst (kleur) per bedrijf"
)
st.plotly_chart(fig_treemap, use_container_width=True)


# === Extra inzichten: Percentage tijdsbesteding en ROI-ratio ===

# 1. Percentage tijdsbesteding per bedrijf
totale_uren_all = aggregatie_per_bedrijf["totaal_uren"].sum()
aggregatie_per_bedrijf["% tijdsbesteding"] = (aggregatie_per_bedrijf["totaal_uren"] / totale_uren_all * 100).round(1)

# 2. Verwachte opbrengst berekenen: kostprijs * amount
df_projectlines["sellingprice"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce")
df_projectlines["amount"] = pd.to_numeric(df_projectlines["amount"], errors="coerce")
df_projectlines["verwachte_opbrengst"] = df_projectlines["sellingprice"] * df_projectlines["amount"]

verwachte_opbrengst_per_bedrijf = df_projectlines.groupby("bedrijf_id")["verwachte_opbrengst"].sum().reset_index()
verwachte_opbrengst_per_bedrijf.columns = ["bedrijf_id", "verwachte_opbrengst"]

aggregatie_per_bedrijf = aggregatie_per_bedrijf.merge(verwachte_opbrengst_per_bedrijf, on="bedrijf_id", how="left")

aggregatie_per_bedrijf["ROI_ratio"] = (
    aggregatie_per_bedrijf["werkelijke_opbrengst"] / aggregatie_per_bedrijf["verwachte_opbrengst"]
).round(2)

# Filter: verwijder rijen waarbij ROI_ratio NaN is, verwachte_opbrengst 0 of leeg, of ROI_ratio gelijk aan 0
aggregatie_per_bedrijf = aggregatie_per_bedrijf[
    aggregatie_per_bedrijf["ROI_ratio"].notna() &
    (aggregatie_per_bedrijf["verwachte_opbrengst"] > 0) &
    (aggregatie_per_bedrijf["ROI_ratio"] > 0)
]

# KPI-widgets voor deze extra inzichten
st.markdown("### 🧮 Extra KPI's")

col1, col2 = st.columns(2)
# Alleen afronden bij presentatie, niet in data!
col1.metric("Totale bestede uren", f"{totale_uren_all:.0f} uur")
roi_gem = aggregatie_per_bedrijf["ROI_ratio"].mean()
col2.metric("Gemiddelde ROI-ratio", f"{roi_gem:.2f}")

# Tabel tonen met nieuwe inzichten

st.markdown("### 📋 Bedrijven met % tijdsbesteding en ROI-ratio")
df_extra = aggregatie_per_bedrijf[["bedrijf_naam", "totaal_uren", "% tijdsbesteding", "werkelijke_opbrengst", "verwachte_opbrengst", "ROI_ratio"]]
df_extra = df_extra.dropna(subset=["ROI_ratio"])
# Alleen afronden bij presentatie, niet in data!
st.dataframe(df_extra.sort_values("ROI_ratio", ascending=True).style.format({
    "totaal_uren": "{:.1f}",
    "% tijdsbesteding": "{:.1f}",
    "werkelijke_opbrengst": "€ {:.2f}",
    "verwachte_opbrengst": "€ {:.2f}",
    "ROI_ratio": "{:.2f}"
}), use_container_width=True)

 # === Urenverdeling per bedrijf (percentage van totale uren) ===
st.markdown("### ⏳ Urenverdeling per bedrijf")

df_urenverdeling = aggregatie_per_bedrijf[["bedrijf_naam", "totaal_uren", "% tijdsbesteding"]].copy()
df_urenverdeling = df_urenverdeling.sort_values("% tijdsbesteding", ascending=False)

fig_uren = px.bar(
    df_urenverdeling,
    x="% tijdsbesteding",
    y="bedrijf_naam",
    orientation="h",
    labels={"% tijdsbesteding": "% van totale uren", "bedrijf_naam": "Bedrijf"},
    title="Verdeling van totaal bestede uren per bedrijf",
    height=600
)
fig_uren.update_layout(yaxis={'categoryorder': 'total ascending'}, margin={'l': 150})

st.plotly_chart(fig_uren, use_container_width=True)

# === Pareto-analyse (80/20-regel) ===
st.markdown("### 🧠 Pareto-analyse: Welk aantal bedrijven leveren het meeste op?")

df_pareto = aggregatie_per_bedrijf[["bedrijf_naam", "werkelijke_opbrengst"]].copy()
df_pareto = df_pareto.sort_values("werkelijke_opbrengst", ascending=False).reset_index(drop=True)
df_pareto["cumulatieve_opbrengst"] = df_pareto["werkelijke_opbrengst"].cumsum()
totale_opbrengst = df_pareto["werkelijke_opbrengst"].sum()
df_pareto["cumulatief_percentage"] = (df_pareto["cumulatieve_opbrengst"] / totale_opbrengst * 100).round(2)

fig_pareto = px.line(
    df_pareto,
    x=df_pareto.index + 1,
    y="cumulatief_percentage",
    markers=True,
    labels={"x": "Aantal bedrijven", "cumulatief_percentage": "Cumulatieve opbrengst (%)"},
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
top_klanten = df_rend[(df_rend["rendement_per_uur"] > mediaan_rendement) & (df_rend["totaal_uren"] < 50)].sort_values("rendement_per_uur", ascending=False).head(5)


# === 🔮 AI Forecasting: Verwacht KPI's via regressiemodel ===

import numpy as np

st.markdown("### 🔮 AI Simulatie per Bedrijf – Wat als Scenario's & Aanbevelingen")

# Voorbereiding data voor regressie
reg_data = aggregatie_per_bedrijf[["totaal_uren", "verwachte_opbrengst", "werkelijke_opbrengst", "ROI_ratio", "rendement_per_uur"]].dropna()

X = reg_data[["totaal_uren", "verwachte_opbrengst"]]
y = reg_data["ROI_ratio"]

model = RandomForestRegressor(random_state=42)
model.fit(X, y)

# Feature importance berekenen
feature_importances = model.feature_importances_
importances_df = pd.DataFrame({
    'Feature': X.columns,
    'Importance': feature_importances
}).sort_values(by='Importance', ascending=False)

bedrijven_keuze = aggregatie_per_bedrijf["bedrijf_naam"].dropna().unique()
bedrijf_selectie = st.selectbox("Selecteer een bedrijf", bedrijven_keuze)

bedrijf_data = aggregatie_per_bedrijf[aggregatie_per_bedrijf["bedrijf_naam"] == bedrijf_selectie].iloc[0]
default_uren = int(bedrijf_data["totaal_uren"])
default_opbrengst = int(bedrijf_data["verwachte_opbrengst"])

# Alleen afronden bij presentatie, niet in data!
sim_uren = st.number_input("⚙️ Stel totaal bestede uren in", min_value=1, value=default_uren)
sim_opbrengst = st.number_input("💰 Stel verwachte opbrengst in (€)", min_value=1, value=default_opbrengst)

X_sim = pd.DataFrame([[sim_uren, sim_opbrengst]], columns=['totaal_uren', 'verwachte_opbrengst'])
sim_roi = model.predict(X_sim)[0]

st.metric("📈 Voorspelde ROI-ratio", f"{sim_roi:.2f}")
if sim_roi < 1.0:
    st.warning("⚠️ Verwachte ROI lager dan 1.0 – verlieslatend scenario.")
elif sim_roi < 1.2:
    st.info("ℹ️ ROI is marginaal – overweeg tariefverhoging of urenverlaging.")
else:
    st.success("✅ Verwachte ROI is goed – rendabel project.")

# Suggestie bij lage ROI
if sim_roi < 1.2:
    st.markdown("### 📌 AI-advies:")
    ratio_verbeter = 1.5
    nodig_opbrengst = ratio_verbeter * sim_uren
    extra_opbrengst = nodig_opbrengst - sim_opbrengst
    procent_tariefstijging = (extra_opbrengst / sim_opbrengst * 100)
    st.write(f"📊 Om een ROI van 1.5 te behalen, zou je de opbrengst moeten verhogen met ~€{extra_opbrengst:.0f} → dat is een tariefstijging van {procent_tariefstijging:.1f}%.")

# Elasticiteitsgrafiek
st.markdown("### 📉 Elasticiteitsgrafiek: hoe verandert ROI bij toenemende uren")

uren_range = np.arange(10, 200, 10)
opbrengst = sim_opbrengst  # opbrengst constant
roi_pred = model.predict(
    pd.DataFrame(
        np.column_stack((uren_range, [opbrengst]*len(uren_range))),
        columns=['totaal_uren', 'verwachte_opbrengst']
    )
)

fig_elastic = px.line(x=uren_range, y=roi_pred,
                      labels={"x": "Totaal Uren", "y": "Voorspelde ROI"},
                      title="Elasticiteit van ROI bij variërende uren (opbrengst constant)")
fig_elastic.add_hline(y=1.0, line_dash="dot", line_color="red")
fig_elastic.add_hline(y=1.5, line_dash="dot", line_color="green")
st.plotly_chart(fig_elastic, use_container_width=True)

# === Feature Importance visualisatie ===
st.markdown("### 🔍 Belangrijkste factoren die ROI beïnvloeden (Feature Importance)")

if importances_df is not None:
    fig_importance = px.bar(importances_df, x='Importance', y='Feature', orientation='h',
                            title='Belang van input-variabelen voor ROI voorspelling')
    fig_importance.update_layout(margin={'l': 150})
    st.plotly_chart(fig_importance, use_container_width=True)

 # Pricing engine
st.markdown("### 🧮 Pricing Engine: Minimale opbrengst voor gewenste ROI")

desired_roi = st.slider("Streef-ROI", min_value=1.0, max_value=3.0, step=0.1, value=1.5)
benodigde_opbrengst = sim_uren * desired_roi
huidige_opbrengst = sim_opbrengst
verschil = benodigde_opbrengst - huidige_opbrengst

st.write(f"🔍 Om een ROI van {desired_roi:.1f} te halen bij {sim_uren} uur, is minimaal €{benodigde_opbrengst:.0f} aan opbrengst nodig.")
if verschil > 0:
    stijging_pct = verschil / huidige_opbrengst * 100
    st.warning(f"→ Dat is een stijging van €{verschil:.0f} (+{stijging_pct:.1f}%) t.o.v. huidige opbrengst.")
else:
    st.success("✅ Huidige opbrengst voldoet al aan deze ROI-eis.")



st.markdown("#### 3. Clustering van bedrijven op basis van prestaties")
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

cluster_data = aggregatie_per_bedrijf[["totaal_uren", "werkelijke_opbrengst", "rendement_per_uur"]].dropna()
scaled = StandardScaler().fit_transform(cluster_data)
kmeans = KMeans(n_clusters=4, random_state=42).fit(scaled)
cluster_data["cluster"] = kmeans.labels_
fig_cluster = px.scatter_3d(
    cluster_data,
    x="totaal_uren", y="werkelijke_opbrengst", z="rendement_per_uur",
    color="cluster", title="3D Clustering van Bedrijven",
    hover_name=aggregatie_per_bedrijf.loc[cluster_data.index, "bedrijf_naam"]
)
st.plotly_chart(fig_cluster, use_container_width=True)

 
# === AI-adviseur: automatisch gegenereerd advies per bedrijf op basis van prestaties ===
st.markdown("### 🧠 AI-adviseur: Automatisch gegenereerd advies per bedrijf")

bedrijf_advies = st.selectbox("📌 Kies een bedrijf voor advies", aggregatie_per_bedrijf["bedrijf_naam"].dropna().unique())

bedrijf_info = aggregatie_per_bedrijf[aggregatie_per_bedrijf["bedrijf_naam"] == bedrijf_advies].iloc[0]

advies_prompt = f"""
Je bent een zakelijke AI-consultant. Geef beknopt maar concreet advies voor het volgende bedrijf:
- Naam: {bedrijf_advies}
- Totaal bestede uren: {bedrijf_info['totaal_uren']:.1f}
- Werkelijke opbrengst: €{bedrijf_info['werkelijke_opbrengst']:.2f}
- Verwachte opbrengst: €{bedrijf_info['verwachte_opbrengst']:.2f}
- ROI-ratio: {bedrijf_info['ROI_ratio']:.2f}
- Rendement per uur: €{bedrijf_info['rendement_per_uur']:.2f}
- % tijdsbesteding: {bedrijf_info['% tijdsbesteding']:.1f}%

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
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ Fout bij ophalen van AI-advies: {e}"

# Only generate and display AI advice when the button is pressed
if st.button("Genereer AI-advies"):
    advies_output = genereer_advies(advies_prompt)
    st.info(advies_output)
import streamlit as st
import pandas as pd
import os
import plotly.express as px
import altair as alt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

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

# KPI-cards: top 3 bedrijven met hoogste rendement per uur
st.markdown("### 🥇 Top 3 bedrijven op basis van rendement per uur")
cols = st.columns(3)
for i, (_, row) in enumerate(df_rend.head(3).iterrows()):
    cols[i].metric(
        label=f"{row['bedrijf_naam']}",
        value=f"€ {row['rendement_per_uur']:.2f}/uur"
    )

# KPI-cards: bottom 10 bedrijven met laagste rendement per uur
st.markdown("### 🛑 Bottom 10 bedrijven op basis van rendement per uur")
cols_bottom = st.columns(5)
for i, (_, row) in enumerate(df_rend.tail(10).iterrows()):
    col_index = i % 5
    with cols_bottom[col_index]:
        st.metric(
            label=f"{row['bedrijf_naam']}",
            value=f"€ {row['rendement_per_uur']:.2f}/uur"
        )

# Extra inzichten: gemiddeld rendement, mediaan, en aantal bedrijven onder drempel
gemiddeld_rendement = df_rend["rendement_per_uur"].mean().round(2)
mediaan_rendement = round(df_rend["rendement_per_uur"].median(), 2)
ondergrens = 50  # drempelrendement, aanpasbaar
aantal_slecht = (df_rend["rendement_per_uur"] < ondergrens).sum()

st.markdown("### 📌 Extra Inzichten over Bedrijfsrendement")
col1, col2, col3 = st.columns(3)
col1.metric("Gemiddeld rendement per uur", f"€ {gemiddeld_rendement}")
col2.metric("Mediaan rendement per uur", f"€ {mediaan_rendement}")
col3.metric(f"Aantal bedrijven < €{ondergrens}", f"{aantal_slecht}")

# Horizontale bar chart van rendement per uur per bedrijf
st.markdown("### 📈 Vergelijking rendement per uur per bedrijf")
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
st.dataframe(df_rend, use_container_width=True)

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

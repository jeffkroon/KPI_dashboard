import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import subprocess
import sys
from dotenv import load_dotenv
from datetime import datetime
from typing import cast
import plotly.graph_objects as go
from utils.auth import require_login, require_email_whitelist
from utils.allowed_emails import ALLOWED_EMAILS
from utils.data_loaders import load_data, load_data_df

st.set_page_config(
    page_title="Dunion KPI Dashboard",
    page_icon="images/dunion-logo-def_donker-06.png",
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

# --- REST VAN DASHBOARD PAS NA LOGIN ---

st.logo("images/dunion-logo-def_donker-06.png")
st.title("Dunion KPI Dashboard – Overzicht")

# --- DATABASE VERVERS KNOP ---
st.markdown("### 🔄 Database Verversen")
st.markdown("Klik op de knop hieronder om de database bij te werken met de nieuwste data uit Gripp.")

def run_gripp_api():
    """Voert gripp_api.py uit om de database te verversen"""
    try:
        # Voer gripp_api.py uit als een subprocess
        result = subprocess.run([sys.executable, "gripp_api.py"], 
                              capture_output=True, 
                              text=True, 
                              cwd=os.getcwd(),
                              timeout=2100)  # 35 minuten timeout
        
        if result.returncode == 0:
            st.success("✅ Database succesvol ververst! Alle data is bijgewerkt.")
            st.info("📊 De pagina wordt automatisch herladen om de nieuwe data te tonen.")
            # Wacht even en herlaad dan de pagina
            st.rerun()
        else:
            st.error(f"❌ Fout bij verversen van database: {result.stderr}")
            if result.stdout:
                st.code(result.stdout, language="bash")
    except subprocess.TimeoutExpired:
        st.error("⏰ Database verversen duurde te lang (>5 minuten). Probeer het later opnieuw.")
    except Exception as e:
        st.error(f"❌ Onverwachte fout: {str(e)}")

# Database ververs knop
if st.button("🔄 Database Verversen", type="primary", use_container_width=True):
    with st.spinner("Database wordt ververst... Dit kan enkele minuten duren."):
        run_gripp_api()

st.markdown("---")

# --- FILTERING KNOPPEN VOOR BEDRIJVEN ---
with st.container():
    st.markdown("""
    <style>
    .filter-box {
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1.5rem;
    }
    .filter-box h4 {
        margin-top: 0;
        font-size: 1.4em;
        font-weight: 600;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="filter-box"><h4>🔎 Filter Bedrijven op Type</h4>', unsafe_allow_html=True)
    filter_optie = st.radio(
        "",
        options=["Alle bedrijven", "Eigen bedrijven", "Klanten"],
        index=0,
        horizontal=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

# --- OMZET RADIO KNOP ---
omzet_optie = st.radio("📊 Welke omzet wil je tonen?", options=["Werkelijke omzet (facturen)", "Geplande omzet (offerte)"], index=0, horizontal=True)

filter_primary_tag = None
if filter_optie == "Eigen bedrijven":
    filter_primary_tag = "1 | Eigen webshop(s) / bedrijven"
elif filter_optie == "Klanten":
    filter_primary_tag = "1 | Externe opdrachten / contracten"
# Voor 'Alle bedrijven' laten we filter_primary_tag op None staan.

st.markdown("""
<style>
h1 {
    font-size: 4em !important;
    font-weight: 800 !important;
    margin-bottom: 0.5em !important;
}
</style>
""", unsafe_allow_html=True)

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
df_projects_raw = load_data_df("projects", columns=["id", "company_id", "archived", "totalinclvat", "name"])
if not isinstance(df_projects_raw, pd.DataFrame):
    df_projects_raw = pd.concat(list(df_projects_raw), ignore_index=True)
df_projects_raw["totalinclvat"] = pd.to_numeric(df_projects_raw["totalinclvat"], errors="coerce").fillna(0)

# Filterbare companies dataset: voeg tag_names toe en filter indien nodig
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

# Helperfunctie voor exacte tag match (alleen primaire tag)
def bedrijf_heeft_tag(tag_string, filter_primary_tag):
    if not isinstance(tag_string, str):
        return False
    tags = [t.strip() for t in tag_string.split(",")]
    return filter_primary_tag in tags

if filter_primary_tag:
    df_companies = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, filter_primary_tag))]
elif filter_optie == "Alle bedrijven":
    # Neem alleen bedrijven mee met geldige tags (behalve lege tags)
    df_companies = df_companies[
        (df_companies["tag_names"].notna()) &
        (df_companies["tag_names"].str.strip() != "")
    ]

# --- Bedrijf ID's na filtering ---
bedrijf_ids = df_companies["id"].unique().tolist()

if len(bedrijf_ids) == 0:
    st.warning("Geen bedrijven gevonden voor deze filterkeuze.")
    st.stop()

df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
if not isinstance(df_employees, pd.DataFrame):
    df_employees = pd.concat(list(df_employees), ignore_index=True)
df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "sellingprice"])
if not isinstance(df_projectlines, pd.DataFrame):
    df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "subject"])
if not isinstance(df_invoices, pd.DataFrame):
    df_invoices = pd.concat(list(df_invoices), ignore_index=True)

# --- Filter projectlines en invoices op bedrijf_ids ---
df_projectlines = df_projectlines[df_projectlines["bedrijf_id"].isin(bedrijf_ids)]
df_invoices = df_invoices[df_invoices["company_id"].isin(bedrijf_ids)]


# --- DATA PREP ---
# Zorg dat de juiste kolommen bestaan en numeriek zijn
if 'bedrijf_id' not in df_projectlines.columns and 'company_id' in df_projectlines.columns:
    df_projectlines = df_projectlines.rename(columns={'company_id': 'bedrijf_id'})
if 'companyname' not in df_companies.columns and 'bedrijf_naam' in df_companies.columns:
    df_companies = df_companies.rename(columns={'bedrijf_naam': 'companyname'})
for col in ["amountwritten", "sellingprice"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

 # Bereken totaal uren per bedrijf direct in SQL
uren_per_bedrijf = load_data_df("projectlines_per_company", columns=["bedrijf_id", "SUM(CAST(amountwritten AS FLOAT)) as totaal_uren"], group_by="bedrijf_id")
uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# === Alleen uren van projectonderdelen met unit_searchname == 'uur' ===
df_projectlines_unit = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "amountwritten", "unit_searchname"])
if not isinstance(df_projectlines_unit, pd.DataFrame):
    df_projectlines_unit = pd.concat(list(df_projectlines_unit), ignore_index=True)
df_projectlines_uur = df_projectlines_unit[
    (df_projectlines_unit["unit_searchname"].str.lower() == "uur") &
    (df_projectlines_unit.get("hidefortimewriting", False) == False)
].copy()
uren_per_bedrijf_uur = df_projectlines_uur.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
uren_per_bedrijf_uur.columns = ["bedrijf_id", "totaal_uren_uur"]

# Merge deze gefilterde uren met uren_per_bedrijf (of bedrijfsstats indien aanwezig)
uren_per_bedrijf = uren_per_bedrijf.merge(uren_per_bedrijf_uur, on="bedrijf_id", how="left")

# Bereken totaal gefactureerd per bedrijf direct in SQL
# Neem alle invoices mee, niet alleen fase='Factuur'
factuurbedrag_per_bedrijf = load_data_df("invoices", columns=["company_id", "SUM(CAST(totalpayed AS FLOAT)) as totalpayed"], group_by="company_id")

# Bereken geplande omzet per bedrijf (op basis van offertes/projecten)
geplande_omzet_per_bedrijf = df_projects_raw.groupby("company_id")["totalinclvat"].sum().reset_index()
geplande_omzet_per_bedrijf.rename(columns={"company_id": "bedrijf_id", "totalinclvat": "geplande_omzet"}, inplace=True)

# Zorg dat beide DataFrames een kolom 'bedrijf_id' hebben vóór de merge
if 'company_id' in uren_per_bedrijf.columns:
    uren_per_bedrijf = uren_per_bedrijf.rename(columns={'company_id': 'bedrijf_id'})
if 'company_id' in factuurbedrag_per_bedrijf.columns:
    factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf.rename(columns={'company_id': 'bedrijf_id'})

# Filter uren_per_bedrijf en factuurbedrag_per_bedrijf op bedrijf_ids (voor zekerheid, SQL kan breder zijn)
uren_per_bedrijf = uren_per_bedrijf[uren_per_bedrijf["bedrijf_id"].isin(bedrijf_ids)]
factuurbedrag_per_bedrijf = factuurbedrag_per_bedrijf[factuurbedrag_per_bedrijf["bedrijf_id"].isin(bedrijf_ids)]

# Combineer stats per bedrijf
bedrijfsstats = uren_per_bedrijf.merge(factuurbedrag_per_bedrijf, on="bedrijf_id", how="outer")
bedrijfsstats = bedrijfsstats.merge(df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left")
bedrijfsstats = bedrijfsstats.drop(columns=[col for col in ['id'] if col in bedrijfsstats.columns])
bedrijfsstats["totaal_uren"] = bedrijfsstats["totaal_uren"].fillna(0)
bedrijfsstats["totalpayed"] = bedrijfsstats["totalpayed"].fillna(0)

# Voeg geplande omzet toe aan bedrijfsstats VOORDAT tarief_per_uur wordt berekend
bedrijfsstats = bedrijfsstats.merge(geplande_omzet_per_bedrijf, on="bedrijf_id", how="left")
bedrijfsstats["geplande_omzet"] = pd.to_numeric(bedrijfsstats["geplande_omzet"], errors="coerce").fillna(0)

# Dynamische tariefberekening afhankelijk van omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["totalpayed"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)
else:
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["geplande_omzet"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)

# Filter bedrijfsstats op bedrijf_ids (voor absolute veiligheid)
bedrijfsstats = bedrijfsstats[bedrijfsstats["bedrijf_id"].isin(bedrijf_ids)]

# --- KPI CARDS ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🏢 Bedrijven", len(df_companies))
with col2:
    st.metric("📋 Opdrachten", len(df_projects_raw))
with col3:
    if omzet_optie == "Werkelijke omzet (facturen)":
        omzet = bedrijfsstats["totalpayed"].sum()
        st.metric("💶 Totale Werkelijke Omzet", f"€ {omzet:,.0f}")
    else:
        omzet = bedrijfsstats["geplande_omzet"].sum()
        st.metric("💶 Totale Geplande Omzet", f"€ {omzet:,.0f}")
with col4:
    st.metric("⏰ Projectregels", len(df_projectlines))

st.markdown("---")

# Zet totaalomzet op basis van geselecteerde omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    bedrijfsstats["totaalomzet"] = bedrijfsstats["totalpayed"]
else:
    bedrijfsstats["totaalomzet"] = bedrijfsstats["geplande_omzet"]

# --- SIMPELE KPI'S & LEUKE INZICHTEN ---
st.markdown("""
<style>
/* Label boven de waarde */
.element-container .stMetric label, [data-testid="stMetric"] label {
    font-size: 1.6em !important;
}
/* Waarde (het getal) */
.element-container .stMetric span, [data-testid="stMetricValue"] {
    font-size: 2.5em !important;
}
/* Delta (optioneel, onder het getal) */
[data-testid="stMetricDelta"] {
    font-size: 1.2em !important;
}
</style>
""", unsafe_allow_html=True)

colA, colB = st.columns(2)

# Hoogste tarief per uur (bedrijf)
if isinstance(bedrijfsstats, pd.DataFrame) and "tarief_per_uur" in bedrijfsstats.columns:
    df_tarief = bedrijfsstats[bedrijfsstats["tarief_per_uur"] > 0]
    if not df_tarief.empty:
        hoogste = df_tarief.sort_values(by="tarief_per_uur", ascending=False).iloc[0]  # type: ignore
        naam_hoog = str(hoogste["companyname"]) if pd.notna(hoogste["companyname"]) else "-"
        tarief_hoog = float(hoogste["tarief_per_uur"]) if pd.notna(hoogste["tarief_per_uur"]) else 0
        colA.metric("Hoogste tarief per uur (bedrijf)", naam_hoog, f"€ {tarief_hoog:.2f}")
    else:
        colA.metric("Hoogste tarief per uur (bedrijf)", "-", "€ 0.00")
else:
    colA.metric("Hoogste tarief per uur (bedrijf)", "-", "€ 0.00")

# Laagste tarief per uur (bedrijf)
if isinstance(bedrijfsstats, pd.DataFrame) and "tarief_per_uur" in bedrijfsstats.columns:
    df_tarief = bedrijfsstats[bedrijfsstats["tarief_per_uur"] > 0]
    if not df_tarief.empty:
        laagste = df_tarief.sort_values(by="tarief_per_uur", ascending=True).iloc[0]  # type: ignore
        naam_laag = str(laagste["companyname"]) if pd.notna(laagste["companyname"]) else "-"
        tarief_laag = float(laagste["tarief_per_uur"]) if pd.notna(laagste["tarief_per_uur"]) else 0
        colB.metric("Laagste tarief per uur (bedrijf)", naam_laag, f"€ {tarief_laag:.2f}")
    else:
        colB.metric("Laagste tarief per uur (bedrijf)", "-", "€ 0.00")
else:
    colB.metric("Laagste tarief per uur (bedrijf)", "-", "€ 0.00")

# --- BAR CHART ---
st.subheader("📊 Tarief per uur per bedrijf")
if not isinstance(bedrijfsstats, pd.DataFrame):
    chart_data = pd.DataFrame(bedrijfsstats)
else:
    chart_data = bedrijfsstats.copy()
assert isinstance(chart_data, pd.DataFrame), "chart_data moet een DataFrame zijn"
chart_data = chart_data[chart_data["tarief_per_uur"] > 0].sort_values(by="tarief_per_uur", ascending=False)  # type: ignore
fig = px.bar(
    chart_data,
    x="companyname",
    y="tarief_per_uur",
    labels={"companyname": "Bedrijf", "tarief_per_uur": "Tarief per uur"},
    title="Tarief per uur per bedrijf",
    height=400
)
fig.update_layout(xaxis_tickangle=-45, margin=dict(l=40, r=20, t=60, b=120))
st.plotly_chart(fig, use_container_width=True)

# --- TOP 5 & BOTTOM 5 ---
st.markdown("### 🔝 Top 10 bedrijven – Hoog tarief per uur")
top5 = chart_data.head(10)[["companyname", "tarief_per_uur"]].copy()
top5["tarief_per_uur"] = pd.Series(top5["tarief_per_uur"]).apply(lambda x: f"€ {float(x):,.2f}")
top5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(top5, use_container_width=True)

st.markdown("### 📉 Bottom 10 bedrijven – Laag tarief per uur")
bottom5 = chart_data.sort_values(by="tarief_per_uur").head(10)[["companyname", "tarief_per_uur"]].copy()
bottom5["tarief_per_uur"] = pd.Series(bottom5["tarief_per_uur"]).apply(lambda x: f"€ {float(x):,.2f}")
bottom5.columns = ["Bedrijfsnaam", "Tarief per Uur (€)"]
st.dataframe(bottom5, use_container_width=True)

# --- ZOEK & FACTUREN ---
st.markdown("---")
st.subheader("🔍 Selecteer een bedrijf voor details en facturen")
bedrijf_opties = bedrijfsstats["companyname"].dropna().unique().tolist()
bedrijf_naam_selectie = st.selectbox("Kies een bedrijf:", bedrijf_opties)
bedrijf_id_selectie = bedrijfsstats.loc[bedrijfsstats["companyname"] == bedrijf_naam_selectie, "bedrijf_id"].iloc[0] if bedrijf_naam_selectie else None

if bedrijf_naam_selectie:
    display_df = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_naam_selectie][["bedrijf_id", "companyname", "totalpayed", "totaal_uren", "tarief_per_uur"]].copy()
    assert isinstance(display_df, pd.DataFrame), "display_df moet een DataFrame zijn"
    display_df = display_df.rename(columns={
        "bedrijf_id": "Bedrijf ID",
        "companyname": "Bedrijfsnaam",
        "totalpayed": "Totaal Gefactureerd (€)",
        "totaal_uren": "Totaal Uren",
        "tarief_per_uur": "Tarief per Uur (€)"
    })
    display_df["Totaal Gefactureerd (€)"] = display_df["Totaal Gefactureerd (€)"].apply(lambda x: f"€ {float(x):,.2f}")
    display_df["Tarief per Uur (€)"] = display_df["Tarief per Uur (€)"].apply(lambda x: f"€ {float(x):,.2f}")
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



# --- WHALES PIE CHART: OMZETVERDELING PER BEDRIJF ---
st.markdown("---")
st.subheader("🐋 Onze 'whales': bedrijven met het grootste deel van de omzet")
omzet_per_bedrijf = bedrijfsstats[["companyname", "totaalomzet"]].copy()
omzet_per_bedrijf = omzet_per_bedrijf.groupby("companyname", dropna=False)["totaalomzet"].sum().reset_index()
omzet_per_bedrijf = omzet_per_bedrijf.sort_values(by="totaalomzet", ascending=False)
top10 = omzet_per_bedrijf.head(10)
rest = omzet_per_bedrijf[10:]["totaalomzet"].sum()
labels = top10["companyname"].tolist()
values = top10["totaalomzet"].tolist()
if rest > 0:
    labels.append("Overig")
    values.append(rest)
omzet_label = "Omzet (€)"
y_col = "totaalomzet"

fig_pie = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4, textinfo='label+percent', hovertemplate='%{label}: €%{value:,.0f}<extra></extra>')])
fig_pie.update_layout(title="Omzetverdeling: top 10 bedrijven vs. rest", height=400, margin=dict(l=40, r=20, t=60, b=40))
st.plotly_chart(fig_pie, use_container_width=True)

# --- BEDRIJVEN MET MEESTE UREN: BAR CHART ---
st.markdown("---")
st.subheader("⏰ Bedrijven waar de meeste uren aan zijn geschreven")

uren_per_bedrijf = bedrijfsstats[["companyname", "totaal_uren"]].copy()
uren_per_bedrijf = uren_per_bedrijf.groupby("companyname", dropna=False)["totaal_uren"].sum().reset_index()
uren_per_bedrijf = uren_per_bedrijf.sort_values(by="totaal_uren", ascending=False)
top10_uren = uren_per_bedrijf.head(10)

fig_uren = px.bar(
    top10_uren,
    x="totaal_uren",
    y="companyname",
    orientation="h",
    labels={"totaal_uren": "Totaal Uren", "companyname": "Bedrijf"},
    title="Top 10 bedrijven met meeste geschreven uren"
)
fig_uren.update_layout(yaxis={'categoryorder':'total ascending'}, height=400, margin=dict(l=40, r=20, t=60, b=40))
st.plotly_chart(fig_uren, use_container_width=True)

# --- OVERSCHRIJDINGEN PER PROJECT (offerprojectbase_id) ---

# Zorg dat amount en amountwritten numeriek zijn
for col in ["amount", "amountwritten"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Groepeer per project (offerprojectbase_id)
df_proj_agg = df_projectlines.groupby(
    ["offerprojectbase_id", "bedrijf_id"], dropna=False
).agg(
    geplande_uren=("amount", "sum"),
    geschreven_uren=("amountwritten", "sum")
).reset_index()

# Voeg projectnaam toe
if "id" in df_projects_raw.columns and "name" in df_projects_raw.columns:
    df_proj_agg = df_proj_agg.merge(
        df_projects_raw[["id", "name"]], left_on="offerprojectbase_id", right_on="id", how="left"
    )

# Voeg bedrijfsnaam toe
if "id" in df_companies.columns and "companyname" in df_companies.columns:
    df_proj_agg = df_proj_agg.merge(
        df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left", suffixes=("", "_bedrijf")
    )

# Bereken overschrijding
df_proj_agg["overschrijding_uren"] = df_proj_agg["geschreven_uren"] - df_proj_agg["geplande_uren"]
df_proj_agg["overschrijding_pct"] = (
    (df_proj_agg["overschrijding_uren"] / df_proj_agg["geplande_uren"].replace(0, pd.NA)) * 100
).fillna(0)

# Filter alleen projecten met overschrijding
df_overschrijding = df_proj_agg[df_proj_agg["overschrijding_uren"] > 0].copy()
df_overschrijding = df_overschrijding.sort_values("overschrijding_uren", ascending=False)

# Toon tabel
st.markdown("### 🚨 Opdrachten met overschrijding van geplande uren")
st.dataframe(
    df_overschrijding[["companyname", "name", "geplande_uren", "geschreven_uren", "overschrijding_uren", "overschrijding_pct"]],
    use_container_width=True
)

# Bar chart top 10
if not df_overschrijding.empty:
    top10 = df_overschrijding.head(10)
    fig = px.bar(
        top10,
        x="name",
        y="overschrijding_uren",
        color="companyname",
        labels={"name": "Project", "overschrijding_uren": "Overschrijding (uren)", "companyname": "Bedrijf"},
        title="Top 10 opdrachten met grootste overschrijding"
    )
    st.plotly_chart(fig, use_container_width=True)


st.markdown("""
<hr style="margin-top: 2em; margin-bottom: 0.5em; border: none; border-top: 1px solid #eee;" />
<div style="text-align: center; color: #888; font-size: 1em; margin-bottom: 0.5em;">
    Dunion Dashboard © 2025
</div>
""", unsafe_allow_html=True)
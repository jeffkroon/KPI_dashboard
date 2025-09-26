import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import subprocess
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
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

# Cache clear knop
if st.button("🗑️ Clear Cache", type="secondary", use_container_width=True):
    st.cache_data.clear()
    st.success("Cache cleared!")
    st.rerun()

# Also add a cache clearing option in sidebar for debugging
if st.sidebar.button("🗑️ Clear Cache (Debug)"):
    st.cache_data.clear()
    st.rerun()

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
        "Bedrijfstype",
        options=["Alle bedrijven", "Eigen bedrijven", "Klanten"],
        index=0,
        horizontal=True,
        label_visibility="collapsed"
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

# Streamlit-extras is optioneel en wordt niet gebruikt in deze app

# --- LOAD DATA ---
@st.cache_data(ttl=3600)
def load_base_data():
    """Load base data that doesn't change often"""
    df_projects_raw = load_data_df("projects", columns=["id", "company_id", "archived", "totalinclvat", "name"])
    if not isinstance(df_projects_raw, pd.DataFrame):
        df_projects_raw = pd.concat(list(df_projects_raw), ignore_index=True)
    df_projects_raw["totalinclvat"] = pd.to_numeric(df_projects_raw["totalinclvat"], errors="coerce").fillna(0)
    
    df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    return df_projects_raw, df_companies

df_projects_raw, df_companies = load_base_data()

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

# --- PERIODE FILTER ---
with st.container():
    st.markdown('<div class="filter-box"><h4>📅 Periode Filter</h4>', unsafe_allow_html=True)
    
    max_date = date.today()
    min_date_default = max_date - timedelta(days=30)

    if "app_dashboard_date_range" not in st.session_state:
        st.session_state.app_dashboard_date_range = (min_date_default, max_date)

    start_default, end_default = st.session_state.app_dashboard_date_range
    if isinstance(start_default, datetime):
        start_default = start_default.date()
    if isinstance(end_default, datetime):
        end_default = end_default.date()

    # Clamp defaults to allowed range
    start_default = max(start_default, date(2020, 1, 1))
    end_default = min(end_default, max_date)

    st.session_state.app_dashboard_date_range = (start_default, end_default)

    date_range = st.date_input(
        "📅 Analyseperiode",
        value=(start_default, end_default),
        min_value=date(2020, 1, 1),
        max_value=max_date,
        key="app_dashboard_date_range",
        help="Selecteer de periode die u wilt analyseren."
    )

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = start_default, end_default

    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    st.session_state["dashboard_start_date"] = start_date
    st.session_state["dashboard_end_date"] = end_date

    # Convert to datetime objects for pandas filtering
    start_date_dt = pd.to_datetime(start_date)
    end_date_dt = pd.to_datetime(end_date)

    # Create string versions for SQL queries
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

st.markdown('</div>', unsafe_allow_html=True)

# --- Bedrijf ID's na filtering ---
bedrijf_ids = df_companies["id"].unique().tolist()

if len(bedrijf_ids) == 0:
    st.warning("Geen bedrijven gevonden voor deze filterkeuze.")
    st.stop()

@st.cache_data(ttl=3600)
def load_employees_and_projectlines():
    """Load employees and projectlines data"""
    df_employees = load_data_df("employees", columns=["id", "firstname", "lastname"])
    if not isinstance(df_employees, pd.DataFrame):
        df_employees = pd.concat(list(df_employees), ignore_index=True)
    
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "sellingprice", "unit_searchname", "createdon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    return df_employees, df_projectlines

df_employees, df_projectlines = load_employees_and_projectlines()
# Load invoices with date filtering in SQL (like werkverdeling.py)
from utils.data_loaders import get_engine
engine = get_engine()

@st.cache_data(ttl=300)
def load_filtered_invoices(start_date_str, end_date_str, bedrijf_ids):
    # Convert bedrijf_ids to tuple for hashable cache key
    bedrijf_ids_tuple = tuple(sorted(bedrijf_ids)) if bedrijf_ids else ()
    
    invoices_query = f"""
    SELECT id, company_id, fase, totalpayed, status_searchname, number, date_date, reportdate_date, subject
    FROM invoices 
    WHERE reportdate_date BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    df_invoices = pd.read_sql(invoices_query, engine)
    df_invoices = df_invoices[df_invoices["company_id"].isin(bedrijf_ids)]
    
    # Convert totalpayed to numeric
    df_invoices['totalpayed'] = pd.to_numeric(df_invoices['totalpayed'], errors='coerce').fillna(0)
    
    return df_invoices

@st.cache_data(ttl=300)
def load_filtered_projectlines(start_date_str, end_date_str, bedrijf_ids):
    """Load and filter projectlines based on date and company"""
    # Convert bedrijf_ids to tuple for hashable cache key
    bedrijf_ids_tuple = tuple(sorted(bedrijf_ids)) if bedrijf_ids else ()
    
    # Filter projectlines op unit "uur" en bedrijf_ids
    df_projectlines_uren = df_projectlines[
        (df_projectlines["unit_searchname"] == "uur") &
        (df_projectlines["bedrijf_id"].isin(bedrijf_ids))
    ].copy()
    
    # Filter projectlines op geselecteerde periode (als createdon_date beschikbaar is)
    if 'createdon_date' in df_projectlines_uren.columns:
        # Convert createdon_date to datetime
        df_projectlines_uren['createdon_date'] = pd.to_datetime(df_projectlines_uren['createdon_date'], errors='coerce')
        
        # Filter only records with createdon_date in the selected period
        df_projectlines_with_date = df_projectlines_uren[
            (df_projectlines_uren['createdon_date'].notna()) &
            (df_projectlines_uren['createdon_date'] >= pd.to_datetime(start_date_str)) &
            (df_projectlines_uren['createdon_date'] <= pd.to_datetime(end_date_str))
        ]
        
        # Include records without createdon_date (no date filtering for these)
        df_projectlines_without_date = df_projectlines_uren[df_projectlines_uren['createdon_date'].isna()]
        
        # Combine both
        df_projectlines_filtered = pd.concat([df_projectlines_with_date, df_projectlines_without_date], ignore_index=True)
    else:
        # No createdon_date column, use all projectlines
        df_projectlines_filtered = df_projectlines_uren
    
    return df_projectlines_filtered

df_invoices = load_filtered_invoices(start_date_str, end_date_str, bedrijf_ids)
df_projectlines_filtered = load_filtered_projectlines(start_date_str, end_date_str, bedrijf_ids)

# Date filtering is now done in SQL query above


# --- DATA PREP ---
# Zorg dat de juiste kolommen bestaan en numeriek zijn
if 'bedrijf_id' not in df_projectlines.columns and 'company_id' in df_projectlines.columns:
    df_projectlines = df_projectlines.rename(columns={'company_id': 'bedrijf_id'})
if 'companyname' not in df_companies.columns and 'bedrijf_naam' in df_companies.columns:
    df_companies = df_companies.rename(columns={'bedrijf_naam': 'companyname'})
for col in ["amountwritten", "sellingprice"]:
    if col in df_projectlines.columns:
        df_projectlines[col] = pd.to_numeric(df_projectlines[col], errors="coerce")

# Bereken totaal uren per bedrijf met datumfilter via projectlines
# Filter projectlines op unit "uur" en bedrijf_ids
df_projectlines_uren = df_projectlines[
    (df_projectlines["unit_searchname"] == "uur") &
    (df_projectlines["bedrijf_id"].isin(bedrijf_ids))
].copy()

# Filter projectlines op geselecteerde periode (als createdon_date beschikbaar is)
if 'createdon_date' in df_projectlines_uren.columns:
    # Debug: Toon voorbeelden van createdon_date voordat conversie
    st.write(f"🔍 DEBUG: Voorbeelden createdon_date (voor conversie):")
    sample_dates = df_projectlines_uren['createdon_date'].head(5).tolist()
    st.write(sample_dates)
    
    df_projectlines_uren['createdon_date'] = pd.to_datetime(df_projectlines_uren['createdon_date'], errors='coerce')
    
    # Debug: Toon voorbeelden na conversie
    st.write(f"🔍 DEBUG: Voorbeelden createdon_date (na conversie):")
    sample_dates_after = df_projectlines_uren['createdon_date'].head(5).tolist()
    st.write(sample_dates_after)
    
    # Debug: Toon hoeveel null values
    null_count = df_projectlines_uren['createdon_date'].isna().sum()
    st.write(f"🔍 DEBUG: Null values na conversie: {null_count}")
    
    # Alleen records met createdon_date filteren op periode
    df_projectlines_with_date = df_projectlines_uren[
        (df_projectlines_uren['createdon_date'].notna()) &
        (df_projectlines_uren['createdon_date'] >= start_date_dt) &
        (df_projectlines_uren['createdon_date'] <= end_date_dt)
    ]
    # Records zonder createdon_date toevoegen (geen datum filtering)
    df_projectlines_without_date = df_projectlines_uren[df_projectlines_uren['createdon_date'].isna()]
    # Combineer beide
    df_projectlines_filtered = pd.concat([df_projectlines_with_date, df_projectlines_without_date], ignore_index=True)
    
    st.write(f"🔍 DEBUG: Projectlines met datum in periode: {len(df_projectlines_with_date)}")
    st.write(f"🔍 DEBUG: Projectlines zonder datum: {len(df_projectlines_without_date)}")
    st.write(f"🔍 DEBUG: Totaal projectlines na filtering: {len(df_projectlines_filtered)}")
else:
    # Geen createdon_date kolom, gebruik alle projectlines
    df_projectlines_filtered = df_projectlines_uren
    st.write(f"🔍 DEBUG: Geen createdon_date kolom, gebruik alle {len(df_projectlines_filtered)} projectlines")

# Bereken totaal uren per bedrijf
df_projectlines_filtered["amountwritten"] = pd.to_numeric(df_projectlines_filtered["amountwritten"], errors="coerce")
uren_per_bedrijf = df_projectlines_filtered.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]

# Debug informatie - ALTIJD TONEN om te zien wat er gebeurt
with st.expander("🔍 Debug: Data Filtering Info"):
    st.write(f"**Start datum:** {start_date.strftime('%Y-%m-%d')}")
    st.write(f"**Eind datum:** {end_date.strftime('%Y-%m-%d')}")
    st.write(f"**Bedrijf IDs na filtering:** {len(bedrijf_ids)} bedrijven")
    
    # Debug na data loading
    st.write("**Data counts na filtering:**")
    st.write(f"- Invoices: {len(df_invoices)} records")
    st.write(f"- Projectlines (uren): {len(df_projectlines_filtered)} records")
    st.write(f"- Projectlines (totaal): {len(df_projectlines)} records")
    
    # Debug projectlines dates
    if 'createdon_date' in df_projectlines_filtered.columns:
        st.write("**Projectlines date info:**")
        records_with_date = df_projectlines_filtered['createdon_date'].notna().sum()
        records_without_date = df_projectlines_filtered['createdon_date'].isna().sum()
        st.write(f"- Records met createdon_date: {records_with_date}")
        st.write(f"- Records zonder createdon_date: {records_without_date}")
        if records_with_date > 0:
            st.write(f"- Min date: {df_projectlines_filtered['createdon_date'].min()}")
            st.write(f"- Max date: {df_projectlines_filtered['createdon_date'].max()}")
    
    # Debug invoice dates
    if len(df_invoices) > 0:
        st.write("**Invoice date range:**")
        st.write(f"- Min date: {df_invoices['reportdate_date'].min()}")
        st.write(f"- Max date: {df_invoices['reportdate_date'].max()}")
        # Convert to numeric first, then sum
        total_amount = pd.to_numeric(df_invoices['totalpayed'], errors='coerce').sum()
        st.write(f"- Total invoice amount: €{total_amount:,.2f}")
    
    # Debug: Laat ook zien wat er in de RAW data zit (zonder filtering)
    st.write("**RAW data (zonder datum filtering):**")
    df_invoices_raw = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
    if not isinstance(df_invoices_raw, pd.DataFrame):
        df_invoices_raw = pd.concat(list(df_invoices_raw), ignore_index=True)
    df_invoices_raw = df_invoices_raw[df_invoices_raw["company_id"].isin(bedrijf_ids)]
    
    if 'reportdate_date' in df_invoices_raw.columns:
        df_invoices_raw['reportdate_date'] = pd.to_datetime(df_invoices_raw['reportdate_date'], errors='coerce')
        st.write(f"- RAW invoices: {len(df_invoices_raw)} records")
        if len(df_invoices_raw) > 0:
            st.write(f"- RAW invoice date range: {df_invoices_raw['reportdate_date'].min()} tot {df_invoices_raw['reportdate_date'].max()}")
            total_raw = pd.to_numeric(df_invoices_raw['totalpayed'], errors='coerce').sum()
            st.write(f"- RAW total amount: €{total_raw:,.2f}")
    
    if len(df_invoices) == 0:
        st.warning("⚠️ Geen facturen gevonden voor deze periode!")
    if len(df_projectlines_filtered) == 0:
        st.warning("⚠️ Geen projectlines gevonden voor deze periode!")

# === PROJECTLINES UREN LOGICA ===
# We gebruiken nu projectlines amountwritten voor accurate uren data

# Bereken totaal gefactureerd per bedrijf uit gefilterde data
# Gebruik de gefilterde invoices (df_invoices) in plaats van alle invoices
st.write(f"🔍 DEBUG: df_invoices voor factuurbedrag berekening: {len(df_invoices)} records")
if len(df_invoices) > 0:
    st.write(f"🔍 DEBUG: df_invoices kolommen: {df_invoices.columns.tolist()}")
    st.write(f"🔍 DEBUG: df_invoices company_id unieke waarden: {df_invoices['company_id'].nunique()}")
    st.write(f"🔍 DEBUG: df_invoices totalpayed sample: {df_invoices['totalpayed'].head(5).tolist()}")

factuurbedrag_per_bedrijf = df_invoices.groupby("company_id")["totalpayed"].sum().reset_index()
factuurbedrag_per_bedrijf.rename(columns={"company_id": "bedrijf_id", "totalpayed": "totalpayed"}, inplace=True)

# Ensure totalpayed is numeric
factuurbedrag_per_bedrijf["totalpayed"] = pd.to_numeric(factuurbedrag_per_bedrijf["totalpayed"], errors="coerce").fillna(0)

st.write(f"🔍 DEBUG: factuurbedrag_per_bedrijf: {len(factuurbedrag_per_bedrijf)} records")
if len(factuurbedrag_per_bedrijf) > 0:
    st.write(f"🔍 DEBUG: factuurbedrag_per_bedrijf sample: {factuurbedrag_per_bedrijf.head(5).to_dict('records')}")

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
st.write(f"🔍 DEBUG: uren_per_bedrijf: {len(uren_per_bedrijf)} records")
st.write(f"🔍 DEBUG: factuurbedrag_per_bedrijf: {len(factuurbedrag_per_bedrijf)} records")

bedrijfsstats = uren_per_bedrijf.merge(factuurbedrag_per_bedrijf, on="bedrijf_id", how="outer")
bedrijfsstats = bedrijfsstats.merge(df_companies[["id", "companyname"]], left_on="bedrijf_id", right_on="id", how="left")
bedrijfsstats = bedrijfsstats.drop(columns=[col for col in ['id'] if col in bedrijfsstats.columns])
bedrijfsstats["totaal_uren"] = pd.to_numeric(bedrijfsstats["totaal_uren"], errors="coerce").fillna(0)
bedrijfsstats["totalpayed"] = pd.to_numeric(bedrijfsstats["totalpayed"], errors="coerce").fillna(0)

st.write(f"🔍 DEBUG: bedrijfsstats na merge: {len(bedrijfsstats)} records")
if len(bedrijfsstats) > 0:
    st.write(f"🔍 DEBUG: bedrijfsstats kolommen: {bedrijfsstats.columns.tolist()}")
    st.write(f"🔍 DEBUG: bedrijfsstats totalpayed sample: {bedrijfsstats['totalpayed'].head(5).tolist()}")
    st.write(f"🔍 DEBUG: bedrijfsstats totalpayed sum: {bedrijfsstats['totalpayed'].sum()}")

# Voeg geplande omzet toe aan bedrijfsstats VOORDAT tarief_per_uur wordt berekend
bedrijfsstats = bedrijfsstats.merge(geplande_omzet_per_bedrijf, on="bedrijf_id", how="left")
bedrijfsstats["geplande_omzet"] = pd.to_numeric(bedrijfsstats["geplande_omzet"], errors="coerce").fillna(0)

# Dynamische tariefberekening afhankelijk van omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["totalpayed"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0).infer_objects(copy=False)
else:
    bedrijfsstats["tarief_per_uur"] = bedrijfsstats["geplande_omzet"].div(bedrijfsstats["totaal_uren"].replace(0, pd.NA)).fillna(0)

# Filter bedrijfsstats op bedrijf_ids (voor absolute veiligheid)
bedrijfsstats = bedrijfsstats[bedrijfsstats["bedrijf_id"].isin(bedrijf_ids)]

# --- KPI CARDS ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    # Filter bedrijven op basis van gefilterde data
    bedrijven_in_periode = len(bedrijfsstats[bedrijfsstats["totaal_uren"] > 0])
    st.metric("🏢 Bedrijven", bedrijven_in_periode)
with col2:
    # Filter projecten op basis van projecten met uren in de periode
    projecten_in_periode = len(df_projectlines_filtered["offerprojectbase_id"].unique()) if not df_projectlines_filtered.empty else 0
    st.metric("📋 Opdrachten", projecten_in_periode)
with col3:
    if omzet_optie == "Werkelijke omzet (facturen)":
        omzet = pd.to_numeric(bedrijfsstats["totalpayed"], errors="coerce").sum()
        st.write(f"🔍 DEBUG: KPI berekening - omzet_optie: {omzet_optie}")
        st.write(f"🔍 DEBUG: KPI berekening - bedrijfsstats totalpayed sum: {omzet}")
        st.write(f"🔍 DEBUG: KPI berekening - bedrijfsstats records: {len(bedrijfsstats)}")
        st.metric("💶 Totale Werkelijke Omzet", f"€ {omzet:,.0f}")
    else:
        omzet = pd.to_numeric(bedrijfsstats["geplande_omzet"], errors="coerce").sum()
        st.write(f"🔍 DEBUG: KPI berekening - omzet_optie: {omzet_optie}")
        st.write(f"🔍 DEBUG: KPI berekening - bedrijfsstats geplande_omzet sum: {omzet}")
        st.metric("💶 Totale Geplande Omzet", f"€ {omzet:,.0f}")
with col4:
    # Filter projectlines op basis van gefilterde projecten
    projectlines_in_periode = len(df_projectlines_filtered) if not df_projectlines_filtered.empty else 0
    st.metric("⏰ Projectregels", projectlines_in_periode)

st.markdown("---")

# Zet totaalomzet op basis van geselecteerde omzet_optie
if omzet_optie == "Werkelijke omzet (facturen)":
    bedrijfsstats["totaalomzet"] = bedrijfsstats["totalpayed"]
else:
    bedrijfsstats["totaalomzet"] = bedrijfsstats["geplande_omzet"]

# Zorg dat totaalomzet numeriek is
bedrijfsstats["totaalomzet"] = pd.to_numeric(bedrijfsstats["totaalomzet"], errors="coerce").fillna(0)

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
    st.write(f"🔍 DEBUG: Geselecteerd bedrijf: {bedrijf_naam_selectie}")
    st.write(f"🔍 DEBUG: bedrijf_id_selectie: {bedrijf_id_selectie}")
    
    # Debug: toon alle bedrijfsstats voor dit bedrijf
    bedrijf_stats = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_naam_selectie]
    st.write(f"🔍 DEBUG: bedrijf_stats records: {len(bedrijf_stats)}")
    if len(bedrijf_stats) > 0:
        st.write(f"🔍 DEBUG: bedrijf_stats data: {bedrijf_stats.to_dict('records')}")
    
    display_df = bedrijfsstats[bedrijfsstats["companyname"] == bedrijf_naam_selectie][["bedrijf_id", "companyname", "totalpayed", "totaal_uren", "tarief_per_uur"]].copy()
    assert isinstance(display_df, pd.DataFrame), "display_df moet een DataFrame zijn"
    
    st.write(f"🔍 DEBUG: display_df voor formatting: {display_df.to_dict('records')}")
    
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
        display_columns = ["number", "reportdate_date", "status_searchname", "totalpayed", "subject"]
        display_df = facturen_bedrijf[display_columns].copy()
        assert isinstance(display_df, pd.DataFrame), "display_df moet een DataFrame zijn"
        display_df.columns = ["Factuurnummer", "Datum", "Status", "Bedrag (€)", "Onderwerp"]
        display_df["Bedrag (€)"] = display_df["Bedrag (€)"].apply(lambda x: f"€ {float(x):,.2f}" if pd.notna(x) else "€ 0.00")
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
rest = pd.to_numeric(omzet_per_bedrijf[10:]["totaalomzet"], errors="coerce").sum()
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

# --- OVERSCHRIJDINGEN PER PROJECT (urenregistratie vs projectlines) ---

# Laad projectlines voor geplande uren
# === PROJECTLINES UREN ANALYSE ===
# Gebruik projectlines amountwritten voor zowel geplande als werkelijk gewerkte uren
# (in de toekomst kunnen we echte urenregistratie data toevoegen voor werkelijk gewerkte uren)

# Bereken geplande uren per project uit projectlines (amountwritten)
df_planned_uren = df_projectlines_filtered.groupby(
    ["offerprojectbase_id", "bedrijf_id"], dropna=False
).agg(
    geplande_uren=("amountwritten", "sum")
).reset_index()

# Voor nu gebruiken we dezelfde data voor "werkelijk gewerkte uren" 
# (in de toekomst kunnen we dit vervangen door echte urenregistratie data)
df_werkelijk_uren = df_planned_uren.copy()
df_werkelijk_uren.columns = ["offerprojectbase_id", "bedrijf_id", "geschreven_uren"]

# Merge geplande en werkelijk gewerkte uren
df_proj_agg = df_planned_uren.merge(df_werkelijk_uren, on=["offerprojectbase_id", "bedrijf_id"], how="outer")
df_proj_agg["geplande_uren"] = df_proj_agg["geplande_uren"].fillna(0)
df_proj_agg["geschreven_uren"] = df_proj_agg["geschreven_uren"].fillna(0)

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

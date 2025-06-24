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
df_projects = df_projects[df_projects["archived"] != True].copy()
df_projects["totalexclvat"] = pd.to_numeric(df_projects["totalexclvat"], errors="coerce")
df_projects["startdate_date"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
df_projects["enddate_date"] = pd.to_datetime(df_projects["enddate_date"], errors="coerce")

df_employees = pd.DataFrame(load_data("employees"))
df_companies = pd.DataFrame(load_data("companies"))
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
df_projectlines.loc[:, "werkelijke_opbrengst"] = df_projectlines["sellingprice"] * df_projectlines["amountwritten"]  # type: ignore
# Aggregatie per bedrijf
aggregatie_per_bedrijf = pd.DataFrame(df_projectlines.groupby("bedrijf_id").agg({  # type: ignore
    "werkelijke_opbrengst": "sum",
    "amountwritten": "sum"
}).reset_index().copy())
aggregatie_per_bedrijf.columns = ["bedrijf_id", "werkelijke_opbrengst", "totaal_uren"]
# Merge met projecten en vul NaN's op met 0
df_projects = df_projects.merge(aggregatie_per_bedrijf, left_on="company_id", right_on="bedrijf_id", how="left").copy()
# Vul NaN met 0 en converteer naar numeriek met infer_objects(copy=False)
df_projects["werkelijke_opbrengst"] = df_projects["werkelijke_opbrengst"].fillna(0).infer_objects(copy=False)
df_projects["totaal_uren"] = df_projects["totaal_uren"].fillna(0).infer_objects(copy=False)

# --- KPI CARDS ---
kpi_section = st.container()
with kpi_section:
    style_metric_cards(
        background_color="#F0F4F8",
        border_size_px=1,
        border_color="#BBB",
        border_radius_px=8,
        border_left_color="#9AD8E1",
        box_shadow=True
    )
    st.subheader("KPI's Overzicht")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        actieve_projecten = int(df_projects[df_projects["enddate_date"].isna()].shape[0])
        st.metric(label="📁 Actieve Projecten", value=str(actieve_projecten))
    with col2:
        huidig_jaar = datetime.now().year
        df_projects.loc[:, "startdate_dt"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
        projecten_dit_jaar = int(df_projects[df_projects["startdate_dt"].dt.year == huidig_jaar].shape[0])
        st.metric("📅 Projecten dit jaar", str(projecten_dit_jaar))
    with col3:
        uren = f"{float(df_projects['totaal_uren'].sum()):.2f} uur"
        st.metric(label="⌛ Gewerkte Uren", value=str(uren))
    with col4:
        actieve_medewerkers = int(df_employees[df_employees["active"] == True].shape[0])
        st.metric(label="🧑 Actieve Medewerkers", value=str(actieve_medewerkers))

extra_kpis = st.container()
with extra_kpis:
    kpi5, kpi6 = st.columns(2)
    with kpi5:
        if not df_projects.empty:
            topklant_omzet_series = df_projects.groupby("company_searchname")["werkelijke_opbrengst"].sum()
            topklant_row = str(topklant_omzet_series.idxmax())
            topklant_omzet = float(topklant_omzet_series.max())
            st.metric("👑 Topklant Omzet", f"{topklant_row}", f"€ {topklant_omzet:,.2f}")
        else:
            st.metric("👑 Topklant Omzet", "N/A", "€ 0.00")
    with kpi6:
        totale_opbrengst = f"€ {df_projects['werkelijke_opbrengst'].sum():,.2f}"
        st.metric(label="💰 Totale Opbrengst", value=totale_opbrengst)

# --- CHARTS ---
st.subheader("📊 Inzichten")
tabs = st.tabs(["Status", "Topklanten", "Uren & Omzet", "Treemap", "Cumulatief"])

with tabs[0]:
    st.markdown("**🔄 Projectstatus (Fase)**")
    fase_counts = df_projects["phase_searchname"].value_counts().reset_index(name="Aantal")
    fase_counts.columns = ["Fase", "Aantal"]
    fig1 = px.bar(fase_counts, x="Fase", y="Aantal", title="Aantal projecten per fase")
    st.plotly_chart(fig1, use_container_width=True)

with tabs[1]:
    st.markdown("**💼 Top 5 Klanten op Opbrengst**")
    omzet_per_klant = df_projects.groupby("company_searchname", as_index=False)["werkelijke_opbrengst"].sum()
    omzet_per_klant = omzet_per_klant.sort_values(by="werkelijke_opbrengst", ascending=False).head(5)  # type: ignore
    omzet_per_klant.columns = ["Klant", "Opbrengst"]
    fig2 = px.bar(omzet_per_klant, x="Opbrengst", y="Klant", orientation='h', title="Top 5 Klanten (opbrengst uit projectlines)")
    totaal_opbrengst = float(omzet_per_klant["Opbrengst"].sum())
    st.metric("📊 Totaal Opbrengst Top 5", f"€ {totaal_opbrengst:,.2f}")
    st.plotly_chart(fig2, use_container_width=True)

with tabs[2]:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📈 Uren per Maand**")
        df_uren.loc[:, "amount"] = df_uren["amount"].fillna(0).infer_objects(copy=False)
        df_uren.loc[:, "maand"] = pd.to_datetime(df_uren["date_date"], errors="coerce").dt.to_period("M").astype(str)
        uren_per_maand = df_uren.groupby("maand")["amount"].sum().reset_index()
        fig3 = px.line(uren_per_maand, x="maand", y="amount", title="Gewerkte uren per maand")
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        st.markdown("**📉 Omzettrend per Maand**")
        df_projects.loc[:, "maand"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce").dt.to_period("M").astype(str)
        omzet_per_maand = df_projects.groupby("maand")["totalexclvat"].sum().reset_index()
        fig4 = px.line(omzet_per_maand, x="maand", y="totalexclvat", title="Omzet per maand")
        st.plotly_chart(fig4, use_container_width=True)

with tabs[3]:
    st.markdown("**📦 Omzet per klant als treemap**")
    omzet_per_klant_treemap = df_projects.groupby("company_searchname", as_index=False)["werkelijke_opbrengst"].sum()
    omzet_per_klant_treemap = omzet_per_klant_treemap.sort_values("werkelijke_opbrengst", ascending=False)  # type: ignore
    fig5 = px.treemap(omzet_per_klant_treemap, path=["company_searchname"], values="werkelijke_opbrengst", title="📦 Verdeling opbrengst per klant (treemap)")
    st.plotly_chart(fig5, use_container_width=True)

with tabs[4]:
    st.markdown("**📈 Cumulatieve omzetgroei door het jaar**")
    df_projects.loc[:, "startdate"] = pd.to_datetime(df_projects["startdate_date"], errors="coerce")
    df_projects.loc[:, 'jaar'] = df_projects['startdate'].dt.year
    df_projects.loc[:, 'maand'] = df_projects['startdate'].dt.to_period('M').astype(str)
    omzet_growth = df_projects.groupby('maand')["werkelijke_opbrengst"].sum().cumsum().reset_index()
    fig6 = px.area(omzet_growth, x="maand", y="werkelijke_opbrengst", title="📈 Cumulatieve omzetgroei door het jaar")
    st.plotly_chart(fig6, use_container_width=True)


# --- PROJECTOVERZICHT MET FILTERS ---
with st.expander("📁 Projectoverzicht en filters", expanded=True):
    klant_filter = st.multiselect("Klant", options=df_projects["company_searchname"].dropna().unique())
    fase_filter = st.multiselect("Fase", options=df_projects["phase_searchname"].dropna().unique())
    datum_filter = st.date_input("Startdatum vanaf", value=None)

    filtered_df = df_projects.copy()
    if klant_filter:
        filtered_df = pd.DataFrame(filtered_df[filtered_df["company_searchname"].isin(klant_filter)])  # type: ignore
    if fase_filter:
        filtered_df = pd.DataFrame(filtered_df[filtered_df["phase_searchname"].isin(fase_filter)])  # type: ignore
    if datum_filter:
        filtered_df = filtered_df[pd.to_datetime(filtered_df["startdate_date"]) >= pd.to_datetime(datum_filter)]

    display_cols = ["name", "company_searchname", "phase_searchname", "startdate_date", "totalexclvat"]
    st.dataframe(filtered_df[display_cols])


st.subheader("📋 Projectregels per bedrijf")

bedrijf_namen = df_companies["companyname"].dropna().sort_values().tolist()  # type: ignore
gekozen_bedrijf = st.selectbox("📌 Selecteer een bedrijf om de projectlines te bekijken", bedrijf_namen)

bedrijf_info = df_companies[df_companies["companyname"] == gekozen_bedrijf].iloc[0]
bedrijf_id = bedrijf_info["id"]

projectlines = pd.DataFrame(df_projectlines[df_projectlines["bedrijf_id"] == bedrijf_id])  # type: ignore

if not projectlines.empty:
    st.write(f"### 📂 Projectlines voor bedrijf: {gekozen_bedrijf} (ID: {bedrijf_id})")

    display_cols = ["offerprojectbase_id", "searchname", "sellingprice", "amountwritten", "werkelijke_opbrengst"]
    projectlines_display = projectlines[display_cols].copy()

    # Zorg dat offerprojectbase_id ALLEEN strings bevat vóór toevoegen totaalrij
    projectlines_display["offerprojectbase_id"] = projectlines_display["offerprojectbase_id"].astype(str)

    # Voeg totaalrij toe
    total_row = pd.DataFrame({
        "offerprojectbase_id": ["TOTAAL"],
        "searchname": ["TOTAAL"],
        "sellingprice": [""],
        "amountwritten": [projectlines_display["amountwritten"].sum()],
        "werkelijke_opbrengst": [projectlines_display["werkelijke_opbrengst"].sum()]
    })

    projectlines_display = pd.concat([projectlines_display, total_row], ignore_index=True)

    # Sorteer op werkelijke_opbrengst aflopend (TOTAAL onderaan)
    projectlines_display.loc[projectlines_display["offerprojectbase_id"] != "TOTAAL", "werkelijke_opbrengst"] = (
        projectlines_display.loc[projectlines_display["offerprojectbase_id"] != "TOTAAL", "werkelijke_opbrengst"]
        .fillna(0)
        .infer_objects(copy=False)
    )
    projectlines_display = projectlines_display.sort_values(by="werkelijke_opbrengst", ascending=False, na_position='last')  # type: ignore

    # Format numerieke kolommen voor weergave, inclusief euroteken bij sellingprice en werkelijke_opbrengst
    projectlines_display.loc[:, "sellingprice"] = projectlines_display["sellingprice"].apply(
        lambda x: f"€ {x:,.2f}" if pd.notnull(x) and isinstance(x, (int, float)) else str(x) if x == "" else ""
    )
    projectlines_display.loc[:, "amountwritten"] = projectlines_display["amountwritten"].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
    projectlines_display.loc[:, "werkelijke_opbrengst"] = projectlines_display["werkelijke_opbrengst"].apply(lambda x: f"€ {x:,.2f}" if pd.notnull(x) else "")

    st.dataframe(projectlines_display, use_container_width=True)
else:
    st.info("Geen projectregels gevonden voor dit bedrijf.")


df_companies["accountmanager_searchname"] = df_companies["accountmanager_searchname"].astype(str).str.strip()

df_projects = df_projects.merge(
    df_companies[["id", "accountmanager_searchname"]],
    how="left",
    left_on="company_id",
    right_on="id",
    suffixes=("", "_company")
)

# Als project geen eigen accountmanager heeft, gebruik die van het bedrijf
df_projects["projectmanager"] = df_projects["accountmanager_searchname"]
df_projects.loc[df_projects["projectmanager"] == "", "projectmanager"] = df_projects["accountmanager_searchname_company"]
df_projects["projectmanager"] = df_projects["projectmanager"].replace("", pd.NA).fillna("Onbekend")


# --- Bedrijven per Accountmanager ---
st.subheader("📋 Bedrijven per Accountmanager")

# Strip whitespace en drop lege entries
df_companies["accountmanager_searchname"] = df_companies["accountmanager_searchname"].astype(str).str.strip()
df_companies["accountmanager_searchname"] = df_companies["accountmanager_searchname"].replace("", pd.NA)
accountmanagers = df_companies["accountmanager_searchname"].dropna().unique()

gekozen_am = st.selectbox("👤 Kies een accountmanager", sorted(accountmanagers))
bedrijven_van_am = df_companies[df_companies["accountmanager_searchname"] == gekozen_am]

if not bedrijven_van_am.empty:
    st.write(f"### 🏢 Bedrijven onder begeleiding van: {gekozen_am}")
    st.dataframe(bedrijven_van_am[["companyname", "legalname", "customernumber", "email", "phone"]])
else:
    st.info("Geen bedrijven gevonden voor deze accountmanager.")

st.subheader("📊 Accountmanagers: Uren vs Omzet per Bedrijf")

# Merge df_projects (met uren en omzet) met df_companies (voor accountmanager info)
df_aggregatie = df_projects.groupby("company_id").agg({
    "totaal_uren": "sum",
    "werkelijke_opbrengst": "sum"
}).reset_index()

df_aggregatie = df_aggregatie.merge(df_companies[["id", "companyname", "accountmanager_searchname"]], left_on="company_id", right_on="id", how="left")

# Groepeer opnieuw op accountmanager
accountmanager_stats = df_aggregatie.groupby("accountmanager_searchname").agg({
    "totaal_uren": "sum",
    "werkelijke_opbrengst": "sum"
}).reset_index()

accountmanager_stats = accountmanager_stats.sort_values(by="werkelijke_opbrengst", ascending=False)

# Maak barchart
fig_am = px.bar(accountmanager_stats, 
                x="accountmanager_searchname", 
                y=["werkelijke_opbrengst"],
                title="Werkverdeling per accountmanager (omzet)",
                labels={"value": "Totaal", "accountmanager_searchname": "Accountmanager"},
                barmode="group")
st.plotly_chart(fig_am, use_container_width=True)

from openai import OpenAI
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import altair as alt
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

with st.spinner("Data wordt geladen, even geduld..."):
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



    # Maak een tekstuele samenvatting van feiten per bedrijf
    summary_lines = []
    for _, row in aggregatie_per_bedrijf.iterrows():
        line = (
            f"Bedrijf: {row['bedrijf_naam']}\n"
            f" - Werkelijke opbrengst: €{row['werkelijke_opbrengst']:,.2f}\n"
            f" - Totaal uren: {row['totaal_uren']:.1f}\n"
            f" - Rendement per uur: €{row['rendement_per_uur']:.2f}\n"
        )
        summary_lines.append(line)
    summary_text = "\n".join(summary_lines)

    # Stel AI prompt samen
    prompt = f"""
    Je bent een scherpzinnige data-analist die zakelijke KPI's interpreteert. Hier zijn de feiten per bedrijf:

    {summary_text}

    Geef 3 concrete opvallende inzichten of aanbevelingen die een projectmanager of financieel controller kan gebruiken om de bedrijfsvoering te verbeteren. Wees zakelijk, direct en specifiek.
    """

    # Initialiseer OpenAI client
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Verstuur prompt en ontvang antwoord
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "Je bent een KPI-gedreven business analist."},
            {"role": "user", "content": prompt}
        ]
    )

    ai_advies = response.choices[0].message.content

st.markdown("## 🔮 AI Advies gebaseerd op data")
with st.expander("Bekijk AI analyse en aanbevelingen"):
    aanbevelingen = [lijn.strip() for lijn in ai_advies.split('\n') if lijn.strip()]
    filtered_adviezen = []
    for item in aanbevelingen:
        if item and len(item) > 1:
            if item[0].isdigit() and item[1] == '.':
                filtered_adviezen.append(item[2:].strip())
            elif item.startswith('-') or item.startswith('*'):
                filtered_adviezen.append(item[1:].strip())
            else:
                filtered_adviezen.append(item)
    for idx, advies in enumerate(filtered_adviezen):
        st.markdown(f"### Aanbeveling {idx+1}")
        st.info(advies)

import streamlit as st
import pandas as pd
import altair as alt
import re
import sys
import sys
import pysqlite3
sys.modules["sqlite3"] = pysqlite3
from crewai import Task, Crew, Process
from agents.analist_agent import analist_agent
from agents.consultant_agent import consultant_agent, forecaster_agent
from agents.rapporteur_agent import rapporteur_agent
from agents.redacteur_agent import redacteur_agent
from agents.verstuurder_agent import verstuurder_agent
import time
import uuid
import io
import plotly.express as px
import schedule
import threading
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import base64
import matplotlib.pyplot as plt
import openai
import json
import yaml

# --- DATABASE SETUP EN DATA LAADFUNCTIE ---
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")
engine = create_engine(POSTGRES_URL)

def load_data(table_name):
    query = f"SELECT * FROM {table_name};"
    return pd.read_sql(query, con=engine)

st.set_page_config(page_title="AI Team Chat", page_icon="🤖")
st.title("🤖 AI Team Chat – Praat met je digitale collega's")

# Toon het Podobrace-logo alleen in de sidebar, niet op de mainpage
LOGO_URL = "images/dunion-logo-def_donker-06.png"
st.sidebar.image(LOGO_URL, width=160)

st.markdown("""
    <link href="https://fonts.googleapis.com/css?family=Inter:400,600&display=swap" rel="stylesheet">
    <style>
    html, body, .stApp {
        background-color: #f7f9fa !important;
        font-family: 'Inter', 'Segoe UI', Arial, sans-serif !important;
        color: #222 !important;
    }
    .stSidebar {
        background: #fff !important;
        border-right: 1px solid #e0e0e0 !important;
    }
    .stExpander {
        background: #fff !important;
        border-radius: 12px !important;
        border: 1px solid #e0e0e0 !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03) !important;
        margin-bottom: 1em !important;
        animation: fadeIn 0.7s;
    }
    .stExpanderHeader {
        font-weight: 600 !important;
        font-size: 1.1em !important;
        color: #0a84ff !important;
    }
    .stButton>button {
        background: linear-gradient(90deg, #0a84ff 0%, #0056d6 100%) !important;
        color: #fff !important;
        border-radius: 6px !important;
        border: none !important;
        font-weight: 600 !important;
        padding: 0.6em 1.2em !important;
        margin-bottom: 0.5em !important;
        font-size: 1.05em !important;
        transition: background 0.2s, transform 0.1s, box-shadow 0.2s !important;
        box-shadow: 0 2px 8px rgba(10,132,255,0.07) !important;
        animation: fadeIn 0.7s;
    }
    .stButton>button:hover {
        background: linear-gradient(90deg, #0056d6 0%, #0a84ff 100%) !important;
        color: #fff !important;
        transform: scale(1.04);
        box-shadow: 0 4px 16px rgba(10,132,255,0.12) !important;
    }
    .stDownloadButton>button {
        background: #222 !important;
        color: #fff !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
        animation: fadeIn 0.7s;
    }
    .stChatMessage {
        background: #fff !important;
        border-radius: 12px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
        margin-bottom: 1em !important;
        padding: 1em !important;
        animation: fadeIn 0.7s;
    }
    .stTextInput>div>input, .stTextArea>div>textarea {
        border-radius: 7px !important;
        border: 1.5px solid #b0b6be !important;
        background: #f3f4f6 !important;
        color: #222 !important;
        font-size: 1.08em !important;
        padding: 0.55em 1em !important;
        box-shadow: 0 1px 4px rgba(10,132,255,0.04) !important;
        transition: border 0.2s, box-shadow 0.2s;
        min-height: 2.5em !important;
    }
    .stTextInput>div>input:focus, .stTextArea>div>textarea:focus {
        border: 1.5px solid #0a84ff !important;
        outline: none !important;
        box-shadow: 0 2px 8px rgba(10,132,255,0.10) !important;
        background: #fff !important;
    }
    .stTextInput>div>input::placeholder, .stTextArea>div>textarea::placeholder {
        color: #555 !important;
        opacity: 1 !important;
        font-weight: 400 !important;
    }
    .stTextInput>div>input:invalid, .stTextArea>div>textarea:invalid {
        border-color: #b0b6be !important;
    }
    .stMarkdown {
        font-size: 1.08em !important;
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px);}
        to { opacity: 1; transform: translateY(0);}
    }
    /* --- Sidebar inputvelden écht grijskleurig --- */
    .stSidebar .stTextInput input, .stSidebar .stTextArea textarea {
        border-radius: 7px !important;
        border: 1.5px solid #b0b6be !important;
        background: #f3f4f6 !important;
        color: #222 !important;
        font-size: 1.08em !important;
        padding: 0.55em 1em !important;
        box-shadow: 0 1px 4px rgba(10,132,255,0.04) !important;
        transition: border 0.2s, box-shadow 0.2s;
        min-height: 2.5em !important;
    }
    .stSidebar .stTextInput input:focus, .stSidebar .stTextArea textarea:focus {
        border: 1.5px solid #0a84ff !important;
        outline: none !important;
        box-shadow: 0 2px 8px rgba(10,132,255,0.10) !important;
        background: #fff !important;
    }
    .stSidebar .stTextInput input::placeholder, .stSidebar .stTextArea textarea::placeholder {
        color: #555 !important;
        opacity: 1 !important;
        font-weight: 400 !important;
    }
    .stSidebar .stTextInput input:invalid, .stSidebar .stTextArea textarea:invalid {
        border-color: #b0b6be !important;
    }
    </style>
""", unsafe_allow_html=True)

# Session state voor chatgeschiedenis, uploads, rapporten, feedback, bookmarks
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "uploaded_data" not in st.session_state:
    st.session_state.uploaded_data = None
if "last_report" not in st.session_state:
    st.session_state.last_report = None
if "feedback" not in st.session_state:
    st.session_state.feedback = []
if "bookmarks" not in st.session_state:
    st.session_state.bookmarks = []

# --- Sidebar premium layout ---
st.sidebar.markdown("""
    <style>
    .stSidebar .sidebar-content { padding-top: 2.5em !important; }
    .sidebar-section { background: #fff; border-radius: 14px; box-shadow: 0 2px 8px rgba(10,132,255,0.04); padding: 1.2em 1em 1.2em 1em; margin-bottom: 1.2em; }
    .sidebar-section h4 { margin-top: 0; margin-bottom: 0.7em; font-size: 1.13em; color: #0a84ff; font-weight: 600; letter-spacing: -0.5px; }
    </style>
""", unsafe_allow_html=True)

with st.sidebar.expander("📁 Data upload (optioneel)", expanded=True):
    uploaded_file = st.file_uploader("Upload een CSV- of Excel-bestand", type=["csv", "xlsx"])
    if uploaded_file:
        if uploaded_file.name.endswith(".csv"):
            df_upload = pd.read_csv(uploaded_file)
        else:
            df_upload = pd.read_excel(uploaded_file)
        if df_upload.shape[1] < 2:
            st.error("Upload een bestand met minimaal 2 kolommen!")
            df_upload = None
        else:
            st.success("Data succesvol geüpload!")
            st.write(df_upload.head())
    else:
        df_upload = None

# --- DATA UIT DATABASE LADEN ---
try:
    df_projects = load_data("projects")
    df_companies = load_data("companies")
    df_employees = load_data("employees")
    df_invoices = load_data("invoices")
    df_projectlines = load_data("projectlines_per_company")
    # Geef de dataframes door aan de PythonExecutionTool van de analist_agent
    from agents.analist_agent import python_tool
    import agents.analist_agent as analist_mod
    analist_mod.df_projects = df_projects
    analist_mod.df_companies = df_companies
    analist_mod.df_employees = df_employees
    analist_mod.df_invoices = df_invoices
    analist_mod.df_projectlines = df_projectlines
except Exception as e:
    st.error(f"Kon database-data niet laden: {e}")
    df_projects = df_companies = df_employees = df_invoices = df_projectlines = pd.DataFrame()

# --- DATA SELECTIE ---
# Gebruik upload als die er is, anders database-data (voor analyse)
if df_upload is not None:
    analyse_df = df_upload.copy()
    st.info("Je analyseert nu de geüploade data.")
else:
    # Combineer relevante database-data tot één analyse-df (voorbeeld: projecten + bedrijven)
    analyse_df = df_projects.copy() if not df_projects.empty else pd.DataFrame()
    st.info("Je analyseert nu de data uit de database.")

# --- AGENTS EN CREW ---
crew_agents = [analist_agent, consultant_agent, rapporteur_agent, redacteur_agent, verstuurder_agent]
chat_crew = Crew(
    agents=crew_agents,
    process=Process.sequential,
    verbose=False,
)

# --- VISUALISATIE FUNCTIE ---
def generate_visualization_base64():
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6], label="Voorbeeldlijn")
    ax.set_title("Voorbeeld visualisatie")
    ax.set_xlabel("X-as")
    ax.set_ylabel("Y-as")
    ax.legend()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_bytes = buf.read()
    img_b64 = base64.b64encode(img_bytes).decode('utf-8')
    plt.close(fig)
    return img_b64

# --- RAPPORT GENERATIE EN VERSTUREN ---
def genereer_en_verstuur_rapport(user_prompt, to_email, analyse_df, df_projects, df_companies, df_invoices, df_projectlines, download_pdf):
    # 1. Maak de taken aan
    analyse_task = Task(
        description=f"Analyseer de volgende vraag. Gebruik de volledige dataframes en tools voor je analyse: {user_prompt}",
        expected_output="Een diepgaande analyse van de data, met inzichten en opvallende punten.",
        agent=analist_agent
    )
    advies_task = Task(
        description=f"Geef een concreet advies op basis van deze analyse.",
        expected_output="Een kort, krachtig advies voor het management.",
        agent=consultant_agent
    )
    rapport_task = Task(
        description=f"Vat de analyse en het advies samen in zakelijke managementtaal.",
        expected_output="Een zakelijke, heldere samenvatting voor het rapport.",
        agent=rapporteur_agent
    )
    redactie_task = Task(
        description=f"Maak de samenvatting professioneel, helder en in de juiste tone-of-voice.",
        expected_output="Een professioneel geredigeerde tekst.",
        agent=redacteur_agent
    )
    # 2. Crew voor rapportage
    rapport_crew = Crew(
        agents=[analist_agent, consultant_agent, rapporteur_agent, redacteur_agent],
        tasks=[analyse_task, advies_task, rapport_task, redactie_task],
        process=Process.sequential,
        verbose=False,
    )
    results = rapport_crew.kickoff()
    analyse_result = results[0] if isinstance(results, list) and len(results) > 0 else "-"
    advies_result = results[1] if isinstance(results, list) and len(results) > 1 else "-"
    rapport_result = results[2] if isinstance(results, list) and len(results) > 2 else "-"
    redactie_result = results[3] if isinstance(results, list) and len(results) > 3 else "-"
    # 3. Visualisatie (optioneel)
    img_b64 = generate_visualization_base64()
    visualisatie_html = '<img src="cid:visualisatie1" style="max-width:500px; margin:20px 0;">'
    attachments = [{
        "filename": "visualisatie.png",
        "data": img_b64,
        "type": "image/png",
        "cid": "visualisatie1"
    }]
    # 4. KPI-tabel
    n_bedrijven = len(df_companies) if not df_companies.empty else 0
    n_projecten = len(df_projects) if not df_projects.empty else 0
    totaal_omzet = float(pd.to_numeric(df_invoices["totalpayed"], errors="coerce").sum()) if (not df_invoices.empty and "totalpayed" in df_invoices) else 0.0  # type: ignore
    totaal_uren = float(pd.to_numeric(df_projectlines["amountwritten"], errors="coerce").sum()) if (not df_projectlines.empty and "amountwritten" in df_projectlines) else 0.0  # type: ignore
    hoogste_bedrijf_naam = "-"
    hoogste_omzet = 0
    if not df_invoices.empty and "company_id" in df_invoices and "totalpayed" in df_invoices and not df_companies.empty and "id" in df_companies and "companyname" in df_companies:
        df_invoices["totalpayed"] = pd.to_numeric(df_invoices["totalpayed"], errors="coerce")
        omzet_per_bedrijf = df_invoices.groupby("company_id")["totalpayed"].sum()
        if not omzet_per_bedrijf.empty:
            hoogste_bedrijf_id = omzet_per_bedrijf.idxmax()
            hoogste_bedrijf_naam = df_companies.loc[df_companies["id"] == hoogste_bedrijf_id, "companyname"].values[0] if hoogste_bedrijf_id in df_companies["id"].values else "-"
            hoogste_omzet = float(omzet_per_bedrijf.max())
    kpi_html = f"""
    <table style="border-collapse:collapse; margin:20px 0;">
      <tr><th style="text-align:left;">KPI</th><th>Waarde</th></tr>
      <tr><td>Aantal bedrijven</td><td>{n_bedrijven}</td></tr>
      <tr><td>Aantal projecten</td><td>{n_projecten}</td></tr>
      <tr><td>Totaal gefactureerd</td><td>€{totaal_omzet:,.2f}</td></tr>
      <tr><td>Totaal geschreven uren</td><td>{totaal_uren:,.1f}</td></tr>
      <tr><td>Hoogste omzet bedrijf</td><td>{hoogste_bedrijf_naam} (€{hoogste_omzet:,.2f})</td></tr>
    </table>
    """
    # 5. Combineer alles tot een rijk HTML-rapport
    rapport_html = f"""
    <html>
    <body style="font-family:Inter,Arial,sans-serif; color:#222;">
      <h1 style="color:#0a84ff;">AI Management Rapport</h1>
      <h2>Samenvatting</h2>
      <div style="margin-bottom:20px;">{redactie_result}</div>
      <h2>KPI Overzicht</h2>
      {kpi_html}
      <h2>Visualisatie</h2>
      {visualisatie_html}
      <h2>Volledige Analyse</h2>
      <div style="background:#f7f9fa; border-radius:8px; padding:16px;">{analyse_result}</div>
      <h2>Advies</h2>
      <div style="background:#f7f9fa; border-radius:8px; padding:16px;">{advies_result}</div>
    </body>
    </html>
    """
    subject = "AI Management Rapport"
    verstuurder_agent.tools[0]._run(
        subject=subject,
        body=rapport_html,
        to=to_email,
        attachments=attachments
    )

# --- KPI OVERZICHT FUNCTIE ---
def maak_kpi_overzicht(df_projects, df_companies, df_invoices, df_projectlines):
    n_bedrijven = len(df_companies) if not df_companies.empty else 0
    n_projecten = len(df_projects) if not df_projects.empty else 0
    totaal_omzet = float(pd.to_numeric(df_invoices["totalpayed"], errors="coerce").sum()) if (not df_invoices.empty and "totalpayed" in df_invoices) else 0.0  # type: ignore
    totaal_uren = float(pd.to_numeric(df_projectlines["amountwritten"], errors="coerce").sum()) if (not df_projectlines.empty and "amountwritten" in df_projectlines) else 0.0  # type: ignore
    # Hoogste omzet bedrijf
    hoogste_bedrijf_naam = "-"
    hoogste_omzet = 0
    if not df_invoices.empty and "company_id" in df_invoices and "totalpayed" in df_invoices and not df_companies.empty and "id" in df_companies and "companyname" in df_companies:
        df_invoices["totalpayed"] = pd.to_numeric(df_invoices["totalpayed"], errors="coerce")
        omzet_per_bedrijf = df_invoices.groupby("company_id")["totalpayed"].sum()
        if not omzet_per_bedrijf.empty:
            hoogste_bedrijf_id = omzet_per_bedrijf.idxmax()
            hoogste_bedrijf_naam = df_companies.loc[df_companies["id"] == hoogste_bedrijf_id, "companyname"].values[0] if hoogste_bedrijf_id in df_companies["id"].values else "-"
            hoogste_omzet = float(omzet_per_bedrijf.max())
    overzicht = f"""
**KPI Overzicht**
- Aantal bedrijven: {n_bedrijven}
- Aantal projecten: {n_projecten}
- Totaal gefactureerd: €{totaal_omzet:,.2f}
- Totaal geschreven uren: {totaal_uren:,.1f}
- Hoogste omzet bedrijf: {hoogste_bedrijf_naam} (€{hoogste_omzet:,.2f})
"""
    return overzicht

def process_user_input(user_input, analyse_df=None):
    # Geef de agent expliciet de opdracht om SQL te gebruiken voor data-analyses
    instructie = (
        "Beantwoord de volgende vraag door, indien relevant, een SQL-query uit te voeren op de volledige database. "
        "Gebruik je SQL-tool voor alle data-analyses, zodat je altijd met alle data werkt. "
        "Geef het resultaat helder en gestructureerd terug.\n\n"
        "Licht in je antwoord ook altijd kort toe welke datasets/dataframes je hebt gebruikt en welke stappen je hebt genomen om tot het resultaat te komen.\n\n"
    )
    dynamic_task = Task(
        description=f"{instructie}{user_input}",
        expected_output="Een helder, gestructureerd antwoord van het team, met indien nodig een rapport, analyse, visualisatie of actie.",
        agent=analist_agent
    )
    with st.spinner("Het team is aan het werk..."):
        chat_crew.tasks = [dynamic_task]
        result = chat_crew.kickoff()
    return result

# --- CHATGESCHIEDENIS ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
    # Voeg KPI-overzicht als welkomstbericht toe
    kpi_rapport = maak_kpi_overzicht(df_projects, df_companies, df_invoices, df_projectlines)
    st.session_state.chat_history.append({
        "role": "assistant",
        "agent": "AI Team",
        "content": kpi_rapport
    })
if "last_report" not in st.session_state:
    st.session_state.last_report = None
if "bookmarks" not in st.session_state:
    st.session_state.bookmarks = []
if "feedback" not in st.session_state:
    st.session_state.feedback = []

# --- CHATINTERFACE ---
user_input = st.chat_input("Stel je vraag aan het AI-team...")

# --- VOORBEELDVRAAG-BUTTONS ---
voorbeeldvragen = [
    "Welke bedrijven hebben het hoogste rendement per uur?",
    "Welke medewerker heeft de meeste uren geschreven deze maand?",
    "Geef een overzicht van de omzet per project.",
    "Detecteer opvallende trends in de facturatie.",
    "Maak een rapport van de belangrijkste KPI's van deze maand.",
    "Toon het KPI-overzicht."
]
with st.expander("💡 Voorbeeldvragen (klik om te gebruiken)"):
    for vraag in voorbeeldvragen:
        if st.button(vraag, key=f"voorbeeld_{vraag}"):
            st.session_state.chat_history.append({
                "role": "user",
                "agent": "Gebruiker",
                "content": vraag
            })
            result = process_user_input(vraag, analyse_df)
            st.session_state.chat_history.append({
                "role": "assistant",
                "agent": "AI Team",
                "content": result
            })

# --- ALIAS MAPPING UIT YAML ---
def load_alias_mapping():
    try:
        with open("alias_mapping.yaml", "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def save_alias_mapping(mapping):
    with open("alias_mapping.yaml", "w") as f:
        yaml.safe_dump(mapping, f)

EMAIL_ALIASES = load_alias_mapping()

# --- SIDEBAR UI VOOR ALIAS MANAGEMENT ---
with st.sidebar.expander("🔗 E-mail aliassen beheren", expanded=False):
    st.markdown("**Alias → E-mailadres**")
    for alias, email in EMAIL_ALIASES.items():
        col1, col2, col3 = st.columns([3,5,1])
        with col1:
            new_alias = st.text_input(f"Alias_{alias}", value=alias, key=f"alias_{alias}")
        with col2:
            new_email = st.text_input(f"Email_{alias}", value=email, key=f"email_{alias}")
        with col3:
            if st.button("❌", key=f"del_{alias}"):
                EMAIL_ALIASES.pop(alias)
                save_alias_mapping(EMAIL_ALIASES)
                st.experimental_rerun()  # type: ignore[attr-defined]
        if new_alias != alias or new_email != email:
            EMAIL_ALIASES.pop(alias)
            EMAIL_ALIASES[new_alias] = new_email
            save_alias_mapping(EMAIL_ALIASES)
            st.experimental_rerun()  # type: ignore[attr-defined]
    st.markdown("---")
    new_alias = st.text_input("Nieuwe alias", key="new_alias")
    new_email = st.text_input("Nieuw e-mailadres", key="new_email")
    if st.button("Alias toevoegen"):
        if new_alias and new_email:
            EMAIL_ALIASES[new_alias] = new_email
            save_alias_mapping(EMAIL_ALIASES)
            st.experimental_rerun()  # type: ignore[attr-defined]

# --- EMAIL MASKING ---
def mask_email(email):
    if not email or "@" not in email:
        return email
    name, domain = email.split("@", 1)
    if len(name) <= 1:
        masked = "*" + "@" + domain
    else:
        masked = name[0] + "***@" + domain
    return masked

# --- VERBETERDE SYSTEM PROMPT EN PARSER ---
client = openai.OpenAI()  # gebruikt automatisch je OPENAI_API_KEY env var

def parse_user_command(user_input):
    system_prompt = (
        "Je bent een command parser voor een AI-dashboard. "
        "Geef ALTIJD een geldige JSON terug met de volgende structuur. "
        "Herken de intentie van de gebruiker: mailen, downloaden als PDF, tonen in dashboard, bookmarken. "
        "Voorbeelden:\n"
        "Input: 'Mail een KPI-rapport naar jeff@dunion.nl'\n"
        '{"rapport_type": "KPI", "mailen": true, "email": "jeff@dunion.nl", "download_pdf": false, "toon_dashboard": false, "bookmark": false, "opdracht": "Mail een KPI-rapport"}\n'
        "Input: 'Download een analyse van project X als PDF'\n"
        '{"rapport_type": "analyse", "mailen": false, "email": null, "download_pdf": true, "toon_dashboard": false, "bookmark": false, "opdracht": "Download een analyse van project X"}\n'
        "Input: 'Bookmark dit rapport'\n"
        '{"rapport_type": null, "mailen": false, "email": null, "download_pdf": false, "toon_dashboard": false, "bookmark": true, "opdracht": "Bookmark dit rapport"}\n'
        "Input: 'Toon het KPI-overzicht in het dashboard'\n"
        '{"rapport_type": "KPI", "mailen": false, "email": null, "download_pdf": false, "toon_dashboard": true, "bookmark": false, "opdracht": "Toon het KPI-overzicht"}\n'
        f"Input: {user_input}\n"
        "Let op: Zet onbekende velden op null of false."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": system_prompt}]
        )
        content = response.choices[0].message.content
        if content is not None:
            content = content.replace("'", '"')
            result = json.loads(content)
        else:
            st.error("LLM gaf geen antwoord terug.")
            result = {"rapport_type": None, "mailen": False, "email": None, "download_pdf": False, "toon_dashboard": False, "bookmark": False, "opdracht": user_input}
    except Exception as e:
        st.error(f"Kon de opdracht niet goed begrijpen. Probeer het anders te formuleren. ({e})")
        result = {"rapport_type": None, "mailen": False, "email": None, "download_pdf": False, "toon_dashboard": False, "bookmark": False, "opdracht": user_input}
    # Alias-mapping
    if result.get("email") and result["email"] in EMAIL_ALIASES:
        result["email"] = EMAIL_ALIASES[result["email"]]
    return result

# --- VERWERK GEBRUIKERSCHAT ---
if user_input:
    parsed = parse_user_command(user_input)
    to_email = parsed.get("email")
    mailen = parsed.get("mailen", False)
    opdracht = parsed.get("opdracht", user_input)
    download_pdf = parsed.get("download_pdf", False)
    toon_dashboard = parsed.get("toon_dashboard", False)
    bookmark = parsed.get("bookmark", False)
    if mailen and to_email:
        st.session_state.chat_history.append({
            "role": "user",
            "agent": "Gebruiker",
            "content": user_input
        })
        st.info(f"AI-rapport wordt verstuurd naar {mask_email(to_email)} ...")
        genereer_en_verstuur_rapport(
            user_prompt=opdracht,
            to_email=to_email,
            analyse_df=analyse_df,
            df_projects=df_projects,
            df_companies=df_companies,
            df_invoices=df_invoices,
            df_projectlines=df_projectlines,
            download_pdf=download_pdf
        )
        st.success(f"Het rapport is verstuurd naar {mask_email(to_email)}.")
        st.session_state.chat_history.append({
            "role": "assistant",
            "agent": "AI Team",
            "content": f"Het rapport is verstuurd naar {mask_email(to_email)}."
        })
    elif mailen and not to_email:
        st.session_state.chat_history.append({
            "role": "user",
            "agent": "Gebruiker",
            "content": user_input
        })
        st.warning("Naar welk e-mailadres moet het rapport worden gestuurd?")
        st.session_state.chat_history.append({
            "role": "assistant",
            "agent": "AI Team",
            "content": "Naar welk e-mailadres moet het rapport worden gestuurd?"
        })
    elif download_pdf:
        st.success("Download als PDF wordt binnenkort ondersteund!")
        # Hier kun je de PDF-download logica toevoegen
    elif toon_dashboard:
        st.success("Rapport wordt in het dashboard getoond!")
        # Hier kun je logica toevoegen om het rapport in het dashboard te tonen
    elif bookmark:
        st.session_state.bookmarks.append({"agent": "AI Team", "content": opdracht})
        st.success("Rapport is gebookmarkt!")
    else:
        st.session_state.chat_history.append({
            "role": "user",
            "agent": "Gebruiker",
            "content": user_input
        })
        result = process_user_input(opdracht, analyse_df)
        st.session_state.chat_history.append({
            "role": "assistant",
            "agent": "AI Team",
            "content": result
        })

# --- TOON CHATGESCHIEDENIS ---
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(f"**{msg['agent']}**: {msg['content']}")

# --- RAPPORTAGE DOWNLOAD ---
if st.session_state.get("last_report"):
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:  # type: ignore
        pd.DataFrame({"rapport": [st.session_state.last_report]}).to_excel(writer, index=False)
    excel_buffer.seek(0)
    st.download_button(
        label="⬇️ Download rapport als Excel",
        data=excel_buffer.getvalue(),
        file_name="rapport.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# --- BOOKMARKS EN FEEDBACK (optioneel, uitbreidbaar) ---
with st.sidebar.expander("⭐ Bookmarks", expanded=False):
    if st.session_state.bookmarks:
        for bm in st.session_state.bookmarks:
            st.markdown(f"**{bm['agent']}**: {bm['content'][:60]}...")
    else:
        st.info("Nog geen bookmarks.")

with st.sidebar.expander("💬 Feedback", expanded=False):
    if st.session_state.feedback:
        st.write(st.session_state.feedback)
    else:
        st.info("Nog geen feedback ontvangen.")

# --- AUTOMATISCHE KPI-RAPPORTAGE ---
def send_auto_report(to_email):
    # Genereer het KPI-overzicht als rapport
    rapport = maak_kpi_overzicht(df_projects, df_companies, df_invoices, df_projectlines)
    subject = 'Automatisch KPI-rapport'
    # Gebruik je bestaande email-tool (pas aan indien nodig)
    try:
        result = verstuurder_agent.tools[0]._run(subject=subject, body=rapport, to=to_email)
        print('Automatisch rapport verstuurd!', result)
    except Exception as e:
        print(f'Fout bij versturen automatisch rapport: {e}')

def start_scheduler(frequency, time_str, to_email):
    schedule.clear('auto_report')
    if frequency == 'Elke dag':
        schedule.every().day.at(time_str).do(send_auto_report, to_email=to_email).tag('auto_report')
    elif frequency == 'Elke week':
        schedule.every().monday.at(time_str).do(send_auto_report, to_email=to_email).tag('auto_report')
    elif frequency == 'Elke maand':
        schedule.every(4).weeks.at(time_str).do(send_auto_report, to_email=to_email).tag('auto_report')
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()

with st.expander('⚡ Automatische rapportage instellen'):
    st.write('Stel in wanneer automatisch een rapport per e-mail wordt verstuurd (vereist e-mailtool).')
    frequencies = ['Elke dag', 'Elke week', 'Elke maand']
    selected_frequency = st.selectbox('Frequentie', frequencies)
    report_time = st.time_input('Tijdstip', value=pd.to_datetime('09:00').time())
    to_email = st.text_input('E-mailadres ontvanger', value='jouw@email.nl')
    if st.button('Activeer automatische rapportage'):
        if not to_email or '@' not in to_email:
            st.error('Vul een geldig e-mailadres in.')
        else:
            start_scheduler(selected_frequency, report_time.strftime('%H:%M'), to_email)
            st.success(f'Automatische rapportage geactiveerd voor {selected_frequency} om {report_time.strftime("%H:%M")}, naar {to_email}.')

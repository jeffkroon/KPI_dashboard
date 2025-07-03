import streamlit as st
import pandas as pd
import altair as alt
import re
from crewai import Task, Crew, Process
from agents.analist_agent import analist_agent
from agents.consultant_agent import consultant_agent, forecaster_agent
from agents.rapporteur_agent import rapporteur_agent
from agents.redacteur_agent import redacteur_agent
from agents.verstuurder_agent import verstuurder_agent, email_tool
import time
import uuid
import io
import plotly.express as px
import schedule
import threading
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

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

# --- KPI OVERZICHT FUNCTIE ---
def maak_kpi_overzicht(df_projects, df_companies, df_invoices, df_projectlines):
    n_bedrijven = len(df_companies) if not df_companies.empty else 0
    n_projecten = len(df_projects) if not df_projects.empty else 0
    totaal_omzet = df_invoices["totalpayed"].sum() if (not df_invoices.empty and "totalpayed" in df_invoices) else 0
    totaal_uren = df_projectlines["amountwritten"].sum() if (not df_projectlines.empty and "amountwritten" in df_projectlines) else 0
    # Hoogste omzet bedrijf
    hoogste_bedrijf_naam = "-"
    hoogste_omzet = 0
    if not df_invoices.empty and "company_id" in df_invoices and "totalpayed" in df_invoices and not df_companies.empty and "id" in df_companies and "companyname" in df_companies:
        omzet_per_bedrijf = df_invoices.groupby("company_id")["totalpayed"].sum()
        if not omzet_per_bedrijf.empty:
            hoogste_bedrijf_id = omzet_per_bedrijf.idxmax()
            hoogste_bedrijf_naam = df_companies.loc[df_companies["id"] == hoogste_bedrijf_id, "companyname"].values[0] if hoogste_bedrijf_id in df_companies["id"].values else "-"
            hoogste_omzet = omzet_per_bedrijf.max()
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
    if user_input.strip().lower() in ["toon het kpi-overzicht.", "kpi overzicht", "kpi-overzicht", "toon kpi overzicht", "toon het kpi overzicht"]:
        return maak_kpi_overzicht(df_projects, df_companies, df_invoices, df_projectlines)
    context = ""
    if analyse_df is not None and not analyse_df.empty:
        context += f"DATA_AS_DICT: {analyse_df.head(20).to_dict()}\n"
    dynamic_task = Task(
        description=f"{user_input}\n\n{context}",
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

# --- VERWERK GEBRUIKERSCHAT ---
if user_input:
    st.session_state.chat_history.append({
        "role": "user",
        "agent": "Gebruiker",
        "content": user_input
    })
    result = process_user_input(user_input, analyse_df)
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
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
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
from openai import OpenAI
import os
import streamlit as st
import pandas as pd
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from langchain.tools import Tool
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain.agents import initialize_agent
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph
from langchain_core.runnables import Runnable
from langchain_core.messages import HumanMessage, AIMessage
load_dotenv()
importances_df = None

def vraag_is_relevant(vraag, kolommen):
    return any(col.lower() in vraag.lower() for col in kolommen)

st.set_page_config(
    page_title="Customer-analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.logo("images/dunion-logo-def_donker-06.png")

st.title("AI inzichten/uitschieters")

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

    # --- SQL DATABASE CHATBOT ---

    st.markdown("---")
    st.subheader("🧠 Chat direct met je database")

    if "sql_chat_history" not in st.session_state:
        st.session_state.sql_chat_history = []

    sql_user_input = st.text_input("Stel een vraag over je data (SQL-agent):", key="sql_input")

    try:
        db = SQLDatabase.from_uri(POSTGRES_URL)
        llm = ChatOpenAI(temperature=0, model="gpt-4")
        sql_toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        # LangGraph aanpak voor SQL agent
        class SQLChatState:
            def __init__(self):
                self.history = []

        def build_sql_langgraph_agent(llm, tools):
            agent_runnable: Runnable = create_react_agent(llm, tools)
            workflow = StateGraph(SQLChatState)
            workflow.add_node("agent", agent_runnable)
            workflow.set_entry_point("agent")
            workflow.set_finish_point("agent")
            return workflow.compile()

        sql_agent = build_sql_langgraph_agent(llm, sql_toolkit.get_tools())
    except Exception as e:
        st.warning("SQL-agent kon niet worden geïnitialiseerd. Controleer je verbinding of API-sleutel.")
        sql_agent = None

    if sql_user_input and sql_agent:
        try:
            sql_response = sql_agent.run(sql_user_input)
            st.session_state.sql_chat_history.append(("Jij", sql_user_input))
            st.session_state.sql_chat_history.append(("SQL-AI", sql_response))
        except Exception as e:
            st.session_state.sql_chat_history.append(("SQL-AI", f"Er trad een fout op: {e}"))
            st.warning("De SQL-agent kon je vraag niet verwerken. Probeer een andere formulering.")

    for speaker, text in st.session_state.sql_chat_history:
        if speaker == "Jij":
            st.markdown(f"**🧍 Jij:** {text}")
        else:
            st.markdown(f"**🧠 SQL-AI:** {text}")

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

    ai_advies = None
    if st.button("Genereer AI Advies"):
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Je bent een KPI-gedreven business analist."},
                    {"role": "user", "content": prompt}
                ]
            )
            ai_advies = response.choices[0].message.content
        except Exception as e:
            if "RateLimitError" in str(type(e)):
                st.error("🚫 Je hebt de OpenAI-quota bereikt. Probeer het later opnieuw of upgrade je API-limiet.")
            else:
                st.error(f"Er ging iets mis: {e}")

def zoek_bedrijf_op_naam(naam, df=aggregatie_per_bedrijf):
    match = df[df["bedrijf_naam"].str.contains(naam, case=False)]
    return match.to_markdown(index=False)

def totaal_rendement(df=aggregatie_per_bedrijf):
    totaal = df["werkelijke_opbrengst"].sum()
    uren = df["totaal_uren"].sum()
    rendement = totaal / uren if uren != 0 else 0
    return f"Totaal opbrengst: €{totaal:,.2f}\nTotaal uren: {uren:.1f}\nGemiddeld rendement per uur: €{rendement:.2f}"

def top_uren_bedrijven(df=aggregatie_per_bedrijf, n=3):
    top = df.sort_values("totaal_uren", ascending=False).head(n)
    return top[["bedrijf_naam", "totaal_uren"]].to_markdown(index=False)

zoek_tool = Tool(
    name="ZoekBedrijf",
    func=lambda x: zoek_bedrijf_op_naam(x),
    description="Gebruik dit om bedrijven op naam te zoeken en hun KPI's te tonen."
)

rend_tool = Tool(
    name="TotaalRendement",
    func=lambda x: totaal_rendement(),
    description="Gebruik dit om totaalopbrengst, totaal geschreven uren en gemiddeld rendement per uur te krijgen."
)

top_uren_tool = Tool(
    name="TopBedrijvenOpUren",
    func=lambda x: top_uren_bedrijven(),
    description="Gebruik dit om de topbedrijven met de meeste geschreven uren te tonen."
)

# --- CHATBOT-SECTIE ---

st.markdown("---")
st.subheader("🤖 AI Chatbot – Vraag het aan je KPI’s")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

user_input = st.text_input("Stel een vraag over je klanten, omzet of prestaties:", key="chat_input")

# Combineer meerdere tabellen voor rijkere inzichten
data_frames = {
    "bedrijf_metrics": aggregatie_per_bedrijf,
    "projects": df_projects,
    "companies": df_companies,
    "employees": df_employees,
    "urenregistratie": df_uren
}

# Merge alle dataframes naar één indien nodig
# Voor demonstratie beperken we ons tot bedrijf_metrics, maar dit kan uitgebreid worden

full_data = aggregatie_per_bedrijf.copy()  # Later vervangen door volledige merge


# LangGraph state class
class ChatState:
    def __init__(self):
        self.history = []

    def add(self, speaker, text):
        self.history.append((speaker, text))

    def as_markdown(self):
        return "\n".join(f"**{s}:** {t}" for s, t in self.history)

ChatLLM = ChatOpenAI(temperature=0, model="gpt-4", streaming=True)

# LangGraph agent builder
def build_langgraph_agent(llm, tools):
    from langgraph.prebuilt import create_react_agent

    # Use the correct signature for create_react_agent per latest API
    agent_runnable: Runnable = create_react_agent(llm, tools)

    workflow = StateGraph(ChatState)
    workflow.add_node("agent", agent_runnable)
    workflow.set_entry_point("agent")
    workflow.set_finish_point("agent")
    return workflow.compile()

graph_agent = build_langgraph_agent(ChatLLM, [zoek_tool, rend_tool, top_uren_tool])
st.session_state.agent = graph_agent

if user_input:
    try:
        result = st.session_state.agent.invoke(HumanMessage(user_input))
        antwoord = result.content if hasattr(result, "content") else str(result)
        st.session_state.chat_history.append(("Jij", user_input))
        st.session_state.chat_history.append(("AI", antwoord))
    except Exception as e:
        foutmelding = str(e)
        st.session_state.chat_history.append(("AI", f"Er trad een fout op: {foutmelding}"))
        st.warning("De AI kon je vraag niet verwerken. Probeer een concretere formulering.")

# Toon het gesprek
for speaker, text in st.session_state.chat_history:
    if speaker == "Jij":
        st.markdown(f"**🧍 Jij:** {text}")
    else:
        st.markdown(f"**🤖 AI:** {text}")

st.markdown("## 🔮 AI Advies gebaseerd op data")
if ai_advies:
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

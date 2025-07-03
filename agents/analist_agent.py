from dotenv import load_dotenv
load_dotenv()
import os
from crewai import Agent
from crewai.tools import BaseTool
from llm_setup import gpt4
from sql_tool import sql_agent_tool
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
import pandas as pd
import traceback

# Check if SUPABASE_URI is set and valid
SUPABASE_URI = os.getenv("SUPABASE_URI")
if not SUPABASE_URI or not SUPABASE_URI.startswith("postgresql"):
    raise ValueError(
        "SUPABASE_URI environment variable is not set or invalid. "
        "Please set SUPABASE_URI to a valid PostgreSQL URI, e.g., "
        "'postgresql+psycopg2://user:pass@host:port/db'"
    )

# Placeholder for your SQL tool and LLM (replace with actual objects)

db = SQLDatabase.from_uri(SUPABASE_URI)

class CrewAISQLTool(BaseTool):
    def _run(self, query: str):
        try:
            return db.run(query)
        except Exception as e:
            return f"SQL-fout: {e}"

sql_tool = CrewAISQLTool(
    name="SQL Query Tool",
    description="Voer een SQL-query uit op de Supabase/Postgres database en geef de resultaten terug."
)

# Dataframes worden bij het initialiseren van de tool meegegeven (placeholder, want in de app worden ze pas geladen)
df_projects = None
df_companies = None
df_employees = None
df_invoices = None
df_projectlines = None

class PythonExecutionTool(BaseTool):
    name: str = "Python Execution Tool"
    description: str = (
        "Voer een Python (pandas) code-snippet uit op de beschikbare dataframes: "
        "df_projects, df_companies, df_employees, df_invoices, df_projectlines. "
        "Gebruik dit voor analyses die niet handig in SQL kunnen. "
        "Geef de code als string in het 'code' argument."
    )
    def _run(self, code: str):
        global df_projects, df_companies, df_employees, df_invoices, df_projectlines
        local_vars = {
            'df_projects': df_projects,
            'df_companies': df_companies,
            'df_employees': df_employees,
            'df_invoices': df_invoices,
            'df_projectlines': df_projectlines,
            'pd': pd
        }
        try:
            exec_globals = {}
            exec(code, exec_globals, local_vars)
            # Zoek naar een variabele 'result' in local_vars
            result = local_vars.get('result', None)
            if result is not None:
                return str(result)
            else:
                return "Code uitgevoerd, maar geen 'result' variabele gevonden."
        except Exception as e:
            return f"Python-fout: {e}\n{traceback.format_exc()}"

python_tool = PythonExecutionTool()

analist_agent = Agent(
    role="Data Analyst",
    goal="Haalt de meest actuele KPI's per product op uit Supabase door het uitvoeren van efficiënte en betrouwbare SQL-query's.",
    backstory=(
        "Een analytisch sterke data-specialist met diepgaande kennis van de centrale database. "
        "Weet precies welke tabellen en velden relevant zijn, en is bedreven in het schrijven van complexe queries. "
        "Zorgt altijd voor een snelle, correcte en volledige datalevering als basis voor verdere analyse."
    ),
    tools=[sql_tool, python_tool],
    llm=gpt4
) 
from dotenv import load_dotenv
load_dotenv()
import os
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.utilities.sql_database import SQLDatabase
from llm_setup import gpt4


SUPABASE_URI = os.getenv("SUPABASE_URI")
if not SUPABASE_URI or not SUPABASE_URI.startswith("postgresql"):
    raise ValueError(
        "SUPABASE_URI environment variable is not set or invalid. "
        "Please set SUPABASE_URI to a valid PostgreSQL URL, e.g., "
        "'postgresql+psycopg2://user:pass@host:port/db'"
    )

db = SQLDatabase.from_uri(SUPABASE_URI)
sql_agent_tool = create_sql_agent(llm=gpt4, db=db)

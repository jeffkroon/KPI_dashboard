import os
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

# Laad de environment variabelen
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")

engine = create_engine(POSTGRES_URL)
inspector = inspect(engine)

tables = [
    "projects",
    "companies",
    "employees",
    "projectlines_per_company",
    "invoices",
    "urenregistratie"
]

print("\n=== DATABASE KOLONNEN PER TABEL ===\n")
for table in tables:
    try:
        columns = inspector.get_columns(table)
        colnames = [col['name'] for col in columns]
        print(f"Tabel: {table}")
        print(f"Kolommen: {colnames}\n")
    except Exception as e:
        print(f"FOUT bij ophalen van kolommen voor tabel '{table}': {e}\n") 
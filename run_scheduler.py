import schedule
import time
from gripp_api import fetch_gripp_invoices, fetch_gripp_projects, fetch_gripp_employees, fetch_gripp_companies, fetch_gripp_hours_data, upload_uren_to_supabase
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

def job():
    print(f"\n⏰ Running data fetch: {datetime.now()}")
    datasets = {
        "gripp_invoices": fetch_gripp_invoices(),
        "gripp_projects": fetch_gripp_projects(),
        "gripp_employees": fetch_gripp_employees(),
        "gripp_companies": fetch_gripp_companies(),
        "gripp_hours_data": fetch_gripp_hours_data(),
    }

    if not datasets["gripp_hours_data"].empty:
        upload_uren_to_supabase(datasets["gripp_hours_data"].to_dict("records"))

    for name, df in datasets.items():
        df["snapshot_timestamp"] = datetime.now()
        df.to_sql(name, engine, if_exists="append", index=False)
        print(f"✅ {name}: {len(df)} rows opgeslagen.")

if __name__ == "__main__":
    job()
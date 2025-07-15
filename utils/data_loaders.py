import pandas as pd
from typing import Callable, Union, Generator, Iterator
import os
from pathlib import Path
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv

DATA_DIR = Path(__file__).resolve().parent.parent / "data_cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        load_dotenv()
        POSTGRES_URL = os.getenv("POSTGRES_URL")
        if not POSTGRES_URL:
            raise ValueError("POSTGRES_URL is not set in the environment.")
        # Gebruik connection pooling
        _engine = create_engine(POSTGRES_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
    return _engine

# Universele data loader met kolomselectie en optionele query
# Gebruik deze in alle scripts

def load_data(table_name, columns=None, where=None, limit=None, streaming: bool = False, chunksize: int = 10000) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    engine = get_engine()
    if columns:
        col_str = ", ".join(columns)
    else:
        col_str = "*"
    query = f"SELECT {col_str} FROM {table_name}"
    if where:
        query += f" WHERE {where}"
    if limit:
        query += f" LIMIT {limit}"
    if streaming:
        return pd.read_sql(query, con=engine, chunksize=chunksize)
    return pd.read_sql(query, con=engine)

def load_data_df(*args, **kwargs) -> pd.DataFrame:
    df = load_data(*args, **kwargs)
    if not isinstance(df, pd.DataFrame):
        df = pd.concat(list(df), ignore_index=True)
    return df

def save_to_parquet(df: pd.DataFrame, name: str):
    """Sla een DataFrame op als Parquet-bestand in de cachefolder."""
    if df.empty:
        print(f"⚠️ Waarschuwing: DataFrame '{name}' is leeg en wordt niet opgeslagen.")
        return
    path = DATA_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"✅ Data opgeslagen naar: {path}")

def load_from_parquet(name: str) -> pd.DataFrame:
    """Laad een gecached Parquet-bestand als DataFrame."""
    path = DATA_DIR / f"{name}.parquet"
    if not path.exists():
        print(f"⚠️ Geen cache gevonden voor '{name}'.")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    print(f"📂 Data geladen van: {path}")
    return df

def cache_exists(name: str) -> bool:
    """Check of een Parquet-cachebestand bestaat."""
    return (DATA_DIR / f"{name}.parquet").exists()

MAX_CACHE_AGE_MINUTES = 30

def is_cache_valid(name: str) -> bool:
    path = DATA_DIR / f"{name}.parquet"
    if not path.exists():
        return False
    modified_time = path.stat().st_mtime
    age_minutes = (time.time() - modified_time) / 60
    return age_minutes < MAX_CACHE_AGE_MINUTES

def load_or_fetch(name: str, fetch_func: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    if cache_exists(name) and is_cache_valid(name):
        return load_from_parquet(name)
    df = fetch_func()
    if df.empty:
        print(f"⚠️ Waarschuwing: Ophalen van '{name}' faalde of gaf lege data terug.")
        if cache_exists(name):
            print(f"📂 Gebruik oudere cache van '{name}' als fallback.")
            return load_from_parquet(name)
        return pd.DataFrame()
    save_to_parquet(df, name)
    return df
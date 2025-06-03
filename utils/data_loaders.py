import pandas as pd
from typing import Callable
import os
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data_cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)

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


# Nieuw: laadt uit cache of haalt op met opgegeven functie
def load_or_fetch(name: str, fetch_func: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    """Laad een DataFrame uit de cache of haal hem op met een opgegeven functie."""
    if cache_exists(name):
        return load_from_parquet(name)
    df = fetch_func()
    save_to_parquet(df, name)
    return df
#!/usr/bin/env python3
"""
Script om datum velden in projectlines te onderzoeken
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_projectlines_dates():
    """Onderzoek welke datum velden beschikbaar zijn in projectlines"""
    
    print("🔍 Onderzoek: Datum velden in projectlines")
    print("=" * 50)
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company")
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    print(f"📊 Projectlines data: {len(df_projectlines)} records")
    print(f"📋 Kolommen: {list(df_projectlines.columns)}")
    
    # Zoek naar datum-gerelateerde kolommen
    date_columns = [col for col in df_projectlines.columns if 'date' in col.lower() or 'created' in col.lower() or 'modified' in col.lower()]
    print(f"\n📅 Datum-gerelateerde kolommen: {date_columns}")
    
    # Toon voorbeelden van elke datum kolom
    for col in date_columns:
        print(f"\n🔍 {col}:")
        print(f"  - Type: {df_projectlines[col].dtype}")
        print(f"  - Non-null count: {df_projectlines[col].notna().sum()}")
        print(f"  - Unique values: {df_projectlines[col].nunique()}")
        
        # Toon eerste paar waarden
        sample_values = df_projectlines[col].dropna().head(5).tolist()
        print(f"  - Sample values: {sample_values}")
        
        # Als het een datum kolom is, toon range
        if 'date' in col.lower():
            try:
                df_projectlines[col] = pd.to_datetime(df_projectlines[col], errors='coerce')
                min_date = df_projectlines[col].min()
                max_date = df_projectlines[col].max()
                print(f"  - Date range: {min_date} tot {max_date}")
            except:
                print(f"  - Kon niet converteren naar datum")
    
    # Zoek naar andere mogelijke datum velden
    print(f"\n🔍 Andere mogelijke datum velden:")
    other_date_candidates = [col for col in df_projectlines.columns if any(word in col.lower() for word in ['time', 'stamp', 'created', 'modified', 'updated'])]
    print(f"  - {other_date_candidates}")
    
    # Toon voorbeelden van deze velden
    for col in other_date_candidates:
        if col not in date_columns:
            print(f"\n🔍 {col}:")
            print(f"  - Type: {df_projectlines[col].dtype}")
            print(f"  - Non-null count: {df_projectlines[col].notna().sum()}")
            sample_values = df_projectlines[col].dropna().head(3).tolist()
            print(f"  - Sample values: {sample_values}")

if __name__ == "__main__":
    investigate_projectlines_dates()

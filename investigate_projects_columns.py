#!/usr/bin/env python3
"""
Script om beschikbare kolommen in projects tabel te onderzoeken
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_projects_columns():
    """Onderzoek welke kolommen beschikbaar zijn in projects tabel"""
    
    print("🔍 Onderzoek: Beschikbare kolommen in projects tabel")
    print("=" * 50)
    
    # Load projects data zonder specifieke kolommen
    df_projects = load_data_df("projects")
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"📊 Projects data: {len(df_projects)} records")
    print(f"📋 Kolommen: {list(df_projects.columns)}")
    
    # Zoek naar datum-gerelateerde kolommen
    date_columns = [col for col in df_projects.columns if 'date' in col.lower() or 'created' in col.lower() or 'modified' in col.lower() or 'start' in col.lower() or 'end' in col.lower()]
    print(f"\n📅 Datum-gerelateerde kolommen: {date_columns}")
    
    # Toon voorbeelden van elke datum kolom
    for col in date_columns:
        print(f"\n🔍 {col}:")
        print(f"  - Type: {df_projects[col].dtype}")
        print(f"  - Non-null count: {df_projects[col].notna().sum()}")
        print(f"  - Unique values: {df_projects[col].nunique()}")
        
        # Toon eerste paar waarden
        sample_values = df_projects[col].dropna().head(3).tolist()
        print(f"  - Sample values: {sample_values}")
        
        # Als het een datum kolom is, toon range
        if 'date' in col.lower():
            try:
                df_projects[col] = pd.to_datetime(df_projects[col], errors='coerce')
                min_date = df_projects[col].min()
                max_date = df_projects[col].max()
                print(f"  - Date range: {min_date} tot {max_date}")
            except:
                print(f"  - Kon niet converteren naar datum")

if __name__ == "__main__":
    investigate_projects_columns()

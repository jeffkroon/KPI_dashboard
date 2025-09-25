#!/usr/bin/env python3
"""
Script om beschikbare kolommen in invoices tabel te onderzoeken
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_invoices_columns():
    """Onderzoek welke kolommen beschikbaar zijn in invoices tabel"""
    
    print("🔍 Onderzoek: Beschikbare kolommen in invoices tabel")
    print("=" * 50)
    
    # Load invoices data zonder specifieke kolommen
    df_invoices = load_data_df("invoices")
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Invoices data: {len(df_invoices)} records")
    print(f"📋 Kolommen: {list(df_invoices.columns)}")
    
    # Zoek naar datum-gerelateerde kolommen
    date_columns = [col for col in df_invoices.columns if 'date' in col.lower() or 'created' in col.lower() or 'modified' in col.lower()]
    print(f"\n📅 Datum-gerelateerde kolommen: {date_columns}")
    
    # Zoek naar project-gerelateerde kolommen
    project_columns = [col for col in df_invoices.columns if 'project' in col.lower() or 'offer' in col.lower()]
    print(f"\n🔗 Project-gerelateerde kolommen: {project_columns}")
    
    # Toon voorbeelden van elke datum kolom
    for col in date_columns:
        print(f"\n🔍 {col}:")
        print(f"  - Type: {df_invoices[col].dtype}")
        print(f"  - Non-null count: {df_invoices[col].notna().sum()}")
        print(f"  - Unique values: {df_invoices[col].nunique()}")
        
        # Toon eerste paar waarden
        sample_values = df_invoices[col].dropna().head(3).tolist()
        print(f"  - Sample values: {sample_values}")
        
        # Als het een datum kolom is, toon range
        if 'date' in col.lower():
            try:
                df_invoices[col] = pd.to_datetime(df_invoices[col], errors='coerce')
                min_date = df_invoices[col].min()
                max_date = df_invoices[col].max()
                print(f"  - Date range: {min_date} tot {max_date}")
            except:
                print(f"  - Kon niet converteren naar datum")

if __name__ == "__main__":
    investigate_invoices_columns()

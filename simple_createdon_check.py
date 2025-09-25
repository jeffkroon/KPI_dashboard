#!/usr/bin/env python3
"""
Eenvoudig script om te onderzoeken waarom createdon_date verloren gaat
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_createdon_loss():
    """Onderzoek waarom createdon_date verloren gaat"""
    print("🔍 CREATEDON DATE VERLIES ONDERZOEK")
    print("=" * 50)
    
    # Load projectlines_per_company data
    print("📊 Projectlines_per_company laden...")
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "createdon_date", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    print(f"📋 Totaal projectlines: {len(df_projectlines)}")
    
    # Check createdon_date kolom
    if 'createdon_date' in df_projectlines.columns:
        print(f"\n🔍 createdon_date kolom analyse:")
        print(f"- Type: {df_projectlines['createdon_date'].dtype}")
        print(f"- Non-null count: {df_projectlines['createdon_date'].notna().sum()}")
        print(f"- Null count: {df_projectlines['createdon_date'].isna().sum()}")
        print(f"- Coverage: {(df_projectlines['createdon_date'].notna().sum() / len(df_projectlines) * 100):.1f}%")
        
        # Check eerste paar records
        print(f"\n📋 Eerste 5 records createdon_date:")
        for idx, row in df_projectlines.head(5).iterrows():
            print(f"  Record {idx + 1}: {row['createdon_date']}")
    else:
        print("❌ Geen 'createdon_date' kolom gevonden!")
    
    # Check andere kolommen
    print(f"\n📋 Alle kolommen: {list(df_projectlines.columns)}")
    
    # Check unit_searchname
    if 'unit_searchname' in df_projectlines.columns:
        print(f"\n🔍 unit_searchname analyse:")
        unit_counts = df_projectlines['unit_searchname'].value_counts()
        print(f"- Unit types: {dict(unit_counts)}")
    
    # Check amountwritten
    if 'amountwritten' in df_projectlines.columns:
        print(f"\n🔍 amountwritten analyse:")
        df_projectlines['amountwritten'] = pd.to_numeric(df_projectlines['amountwritten'], errors='coerce')
        print(f"- Non-null count: {df_projectlines['amountwritten'].notna().sum()}")
        print(f"- Null count: {df_projectlines['amountwritten'].isna().sum()}")
        print(f"- Total amount: {df_projectlines['amountwritten'].sum():.2f}")

if __name__ == "__main__":
    investigate_createdon_loss()

#!/usr/bin/env python3
"""
Script om updatedon_date te onderzoeken als alternatief voor createdon_date
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_updatedon_date():
    """Onderzoek updatedon_date als alternatief voor createdon_date"""
    
    print("🔍 Onderzoek: updatedon_date als alternatief")
    print("=" * 50)
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "createdon_date", "updatedon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    print(f"📊 Projectlines data: {len(df_projectlines)} records")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines[df_projectlines["unit_searchname"] == "uur"].copy()
    print(f"📊 Projectlines uren: {len(df_projectlines_uren)} records")
    
    # Analyseer createdon_date
    print(f"\n📅 CREATEDON_DATE:")
    df_projectlines_uren['createdon_date'] = pd.to_datetime(df_projectlines_uren['createdon_date'], errors='coerce')
    records_with_createdon = df_projectlines_uren['createdon_date'].notna().sum()
    records_without_createdon = df_projectlines_uren['createdon_date'].isna().sum()
    print(f"- Records met createdon_date: {records_with_createdon}")
    print(f"- Records zonder createdon_date: {records_without_createdon}")
    
    if records_with_createdon > 0:
        print(f"- Min createdon_date: {df_projectlines_uren['createdon_date'].min()}")
        print(f"- Max createdon_date: {df_projectlines_uren['createdon_date'].max()}")
    
    # Analyseer updatedon_date
    print(f"\n📅 UPDATEDON_DATE:")
    df_projectlines_uren['updatedon_date'] = pd.to_datetime(df_projectlines_uren['updatedon_date'], errors='coerce')
    records_with_updatedon = df_projectlines_uren['updatedon_date'].notna().sum()
    records_without_updatedon = df_projectlines_uren['updatedon_date'].isna().sum()
    print(f"- Records met updatedon_date: {records_with_updatedon}")
    print(f"- Records zonder updatedon_date: {records_without_updatedon}")
    
    if records_with_updatedon > 0:
        print(f"- Min updatedon_date: {df_projectlines_uren['updatedon_date'].min()}")
        print(f"- Max updatedon_date: {df_projectlines_uren['updatedon_date'].max()}")
    
    # Vergelijk beide
    print(f"\n🔍 VERGELIJKING:")
    print(f"- createdon_date coverage: {records_with_createdon/len(df_projectlines_uren)*100:.1f}%")
    print(f"- updatedon_date coverage: {records_with_updatedon/len(df_projectlines_uren)*100:.1f}%")
    
    # Toon voorbeelden
    if records_with_createdon > 0:
        print(f"\n📋 VOORBEELDEN CREATEDON_DATE:")
        sample_createdon = df_projectlines_uren[df_projectlines_uren['createdon_date'].notna()].head(3)
        for idx, row in sample_createdon.iterrows():
            print(f"  - ID {row['id']}: {row['createdon_date']} (amountwritten: {row['amountwritten']})")
    
    if records_with_updatedon > 0:
        print(f"\n📋 VOORBEELDEN UPDATEDON_DATE:")
        sample_updatedon = df_projectlines_uren[df_projectlines_uren['updatedon_date'].notna()].head(3)
        for idx, row in sample_updatedon.iterrows():
            print(f"  - ID {row['id']}: {row['updatedon_date']} (amountwritten: {row['amountwritten']})")
    
    # Aanbeveling
    print(f"\n💡 AANBEVELING:")
    if records_with_updatedon > records_with_createdon:
        print(f"- Gebruik updatedon_date (betere coverage: {records_with_updatedon/len(df_projectlines_uren)*100:.1f}%)")
    elif records_with_createdon > records_with_updatedon:
        print(f"- Gebruik createdon_date (betere coverage: {records_with_createdon/len(df_projectlines_uren)*100:.1f}%)")
    else:
        print(f"- Beide hebben dezelfde coverage, gebruik updatedon_date (meer recent)")

if __name__ == "__main__":
    investigate_updatedon_date()

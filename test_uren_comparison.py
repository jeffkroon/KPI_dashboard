#!/usr/bin/env python3
"""
Test script om te controleren of uren uit urenregistratie en projectlines_per_company overeenkomen
"""

import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils.data_loaders import load_data_df

def test_uren_comparison():
    """Test of uren uit beide bronnen overeenkomen"""
    
    print("🔍 Test: Uren vergelijking tussen urenregistratie en projectlines_per_company")
    print("=" * 80)
    
    # Load data
    print("\n📊 Data laden...")
    
    # 1. Urenregistratie data
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    print(f"✅ Urenregistratie geladen: {len(df_urenregistratie)} records")
    
    # 2. Projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    print(f"✅ Projectlines geladen: {len(df_projectlines)} records")
    
    # 3. Projects data voor mapping
    df_projects = load_data_df("projects", columns=["id", "company_id"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"✅ Projects geladen: {len(df_projects)} records")
    
    # Filter urenregistratie op gefiatteerde uren
    print("\n🔍 Filtering urenregistratie...")
    df_uren_filtered = df_urenregistratie[df_urenregistratie['status_searchname'] == 'Gefiatteerd'].copy()
    print(f"✅ Gefiatteerde uren: {len(df_uren_filtered)} records")
    
    # Filter projectlines op uren
    print("\n🔍 Filtering projectlines...")
    df_projectlines_uren = df_projectlines[df_projectlines['unit_searchname'].str.lower() == 'uur'].copy()
    print(f"✅ Projectlines met uren: {len(df_projectlines_uren)} records")
    
    # Merge urenregistratie met projects om company_id te krijgen
    print("\n🔗 Merging urenregistratie met projects...")
    df_uren_with_company = df_uren_filtered.merge(
        df_projects, 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    print(f"✅ Urenregistratie met company_id: {len(df_uren_with_company)} records")
    
    # Bereken totaal uren per bedrijf uit urenregistratie
    print("\n📊 Berekenen totaal uren per bedrijf...")
    df_uren_with_company['amount'] = pd.to_numeric(df_uren_with_company['amount'], errors='coerce')
    uren_per_bedrijf_urenregistratie = df_uren_with_company.groupby('company_id')['amount'].sum().reset_index()
    uren_per_bedrijf_urenregistratie.columns = ['bedrijf_id', 'totaal_uren_urenregistratie']
    
    print(f"✅ Bedrijven met urenregistratie: {len(uren_per_bedrijf_urenregistratie)}")
    
    # Bereken totaal uren per bedrijf uit projectlines
    df_projectlines_uren['amountwritten'] = pd.to_numeric(df_projectlines_uren['amountwritten'], errors='coerce')
    uren_per_bedrijf_projectlines = df_projectlines_uren.groupby('bedrijf_id')['amountwritten'].sum().reset_index()
    uren_per_bedrijf_projectlines.columns = ['bedrijf_id', 'totaal_uren_projectlines']
    
    print(f"✅ Bedrijven met projectlines: {len(uren_per_bedrijf_projectlines)}")
    
    # Merge beide resultaten
    print("\n🔗 Vergelijken resultaten...")
    comparison = uren_per_bedrijf_urenregistratie.merge(
        uren_per_bedrijf_projectlines, 
        on='bedrijf_id', 
        how='outer'
    )
    
    # Fill NaN with 0
    comparison['totaal_uren_urenregistratie'] = comparison['totaal_uren_urenregistratie'].fillna(0)
    comparison['totaal_uren_projectlines'] = comparison['totaal_uren_projectlines'].fillna(0)
    
    # Bereken verschil
    comparison['verschil'] = comparison['totaal_uren_urenregistratie'] - comparison['totaal_uren_projectlines']
    comparison['verschil_pct'] = (comparison['verschil'] / comparison['totaal_uren_projectlines'].replace(0, 1)) * 100
    
    print(f"\n📊 RESULTATEN:")
    print(f"Bedrijven met beide bronnen: {len(comparison)}")
    print(f"Totaal uren uit urenregistratie: {comparison['totaal_uren_urenregistratie'].sum():,.2f}")
    print(f"Totaal uren uit projectlines: {comparison['totaal_uren_projectlines'].sum():,.2f}")
    print(f"Totaal verschil: {comparison['verschil'].sum():,.2f}")
    
    # Top 10 verschillen
    print(f"\n🔝 TOP 10 GROOTSTE VERSCHILLEN:")
    top_diff = comparison.nlargest(10, 'verschil')[['bedrijf_id', 'totaal_uren_urenregistratie', 'totaal_uren_projectlines', 'verschil', 'verschil_pct']]
    print(top_diff.to_string(index=False))
    
    # Bedrijven met alleen urenregistratie
    only_urenregistratie = comparison[comparison['totaal_uren_projectlines'] == 0]
    if len(only_urenregistratie) > 0:
        print(f"\n⚠️ Bedrijven met alleen urenregistratie ({len(only_urenregistratie)}):")
        print(only_urenregistratie[['bedrijf_id', 'totaal_uren_urenregistratie']].to_string(index=False))
    
    # Bedrijven met alleen projectlines
    only_projectlines = comparison[comparison['totaal_uren_urenregistratie'] == 0]
    if len(only_projectlines) > 0:
        print(f"\n⚠️ Bedrijven met alleen projectlines ({len(only_projectlines)}):")
        print(only_projectlines[['bedrijf_id', 'totaal_uren_projectlines']].to_string(index=False))
    
    # Samenvatting
    print(f"\n📋 SAMENVATTING:")
    print(f"- Bedrijven met beide bronnen: {len(comparison[(comparison['totaal_uren_urenregistratie'] > 0) & (comparison['totaal_uren_projectlines'] > 0)])}")
    print(f"- Bedrijven met alleen urenregistratie: {len(only_urenregistratie)}")
    print(f"- Bedrijven met alleen projectlines: {len(only_projectlines)}")
    print(f"- Gemiddeld verschil: {comparison['verschil'].mean():,.2f} uren")
    print(f"- Mediaan verschil: {comparison['verschil'].median():,.2f} uren")

if __name__ == "__main__":
    test_uren_comparison()

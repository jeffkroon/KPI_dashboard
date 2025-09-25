#!/usr/bin/env python3
"""
Script om te zien welke bedrijven alleen projectlines hebben (geen urenregistratie)
"""

import pandas as pd
from utils.data_loaders import load_data_df

def show_companies_with_only_projectlines():
    """Toon bedrijven die alleen projectlines hebben"""
    
    print("🏢 Bedrijven met alleen projectlines (geen urenregistratie)")
    print("=" * 60)
    
    # Load companies data
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    # Load urenregistratie data
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    # Load projects for mapping
    df_projects = load_data_df("projects", columns=["id", "company_id"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    # Filter projectlines op uren
    df_projectlines_uren = df_projectlines[df_projectlines['unit_searchname'].str.lower() == 'uur'].copy()
    df_projectlines_uren['amountwritten'] = pd.to_numeric(df_projectlines_uren['amountwritten'], errors='coerce')
    
    # Bereken uren per bedrijf uit projectlines
    uren_per_bedrijf_projectlines = df_projectlines_uren.groupby('bedrijf_id')['amountwritten'].sum().reset_index()
    uren_per_bedrijf_projectlines.columns = ['bedrijf_id', 'totaal_uren_projectlines']
    
    # Filter urenregistratie op gefiatteerde uren
    df_uren_filtered = df_urenregistratie[df_urenregistratie['status_searchname'] == 'Gefiatteerd'].copy()
    
    # Merge urenregistratie met projects om company_id te krijgen
    df_uren_with_company = df_uren_filtered.merge(
        df_projects, 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    # Bereken uren per bedrijf uit urenregistratie
    df_uren_with_company['amount'] = pd.to_numeric(df_uren_with_company['amount'], errors='coerce')
    uren_per_bedrijf_urenregistratie = df_uren_with_company.groupby('company_id')['amount'].sum().reset_index()
    uren_per_bedrijf_urenregistratie.columns = ['bedrijf_id', 'totaal_uren_urenregistratie']
    
    # Merge beide resultaten
    comparison = uren_per_bedrijf_projectlines.merge(
        uren_per_bedrijf_urenregistratie, 
        on='bedrijf_id', 
        how='left'
    )
    
    # Fill NaN with 0
    comparison['totaal_uren_urenregistratie'] = comparison['totaal_uren_urenregistratie'].fillna(0)
    
    # Bedrijven met alleen projectlines
    only_projectlines = comparison[comparison['totaal_uren_urenregistratie'] == 0].copy()
    
    # Merge met company names
    only_projectlines_with_names = only_projectlines.merge(
        df_companies, 
        left_on='bedrijf_id', 
        right_on='id', 
        how='left'
    )
    
    # Sort by hours
    only_projectlines_with_names = only_projectlines_with_names.sort_values('totaal_uren_projectlines', ascending=False)
    
    print(f"\n📊 Totaal {len(only_projectlines_with_names)} bedrijven met alleen projectlines:")
    print()
    
    for _, row in only_projectlines_with_names.iterrows():
        company_name = row['companyname'] if pd.notna(row['companyname']) else 'Onbekend'
        print(f"🏢 {company_name} (ID: {row['bedrijf_id']}) - {row['totaal_uren_projectlines']:,.2f} uren")
    
    print(f"\n📈 Totaal uren in projectlines: {only_projectlines_with_names['totaal_uren_projectlines'].sum():,.2f}")
    print(f"📈 Gemiddeld per bedrijf: {only_projectlines_with_names['totaal_uren_projectlines'].mean():,.2f} uren")

if __name__ == "__main__":
    show_companies_with_only_projectlines()

#!/usr/bin/env python3
"""
Script om Keepershandschoenen.nl te analyseren zoals Korff Dakwerken
"""

import pandas as pd
from utils.data_loaders import load_data_df

def analyze_keepershandschoenen():
    """Analyseer Keepershandschoenen.nl"""
    
    print("🥅 Keepershandschoenen.nl - Analyse")
    print("=" * 50)
    
    # Load companies data
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    # Find Keepershandschoenen.nl
    keepers_company = df_companies[df_companies['companyname'].str.contains('keepershandschoenen', case=False, na=False)]
    if len(keepers_company) == 0:
        print("❌ Keepershandschoenen.nl niet gevonden!")
        return
    
    keepers_id = keepers_company.iloc[0]['id']
    keepers_name = keepers_company.iloc[0]['companyname']
    
    print(f"✅ Gevonden: {keepers_name} (ID: {keepers_id})")
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "hidefortimewriting"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    # Load urenregistratie data
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    # Load projects for mapping
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"\n📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie)} records")
    print(f"- Projects: {len(df_projects)} records")
    
    # Filter projectlines voor Keepershandschoenen
    df_projectlines_keepers = df_projectlines[df_projectlines['bedrijf_id'] == keepers_id].copy()
    df_projectlines_keepers['amount'] = pd.to_numeric(df_projectlines_keepers['amount'], errors='coerce')
    df_projectlines_keepers['amountwritten'] = pd.to_numeric(df_projectlines_keepers['amountwritten'], errors='coerce')
    
    print(f"\n🔍 Projectlines voor {keepers_name}:")
    print(f"- Totaal records: {len(df_projectlines_keepers)}")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines_keepers[df_projectlines_keepers['unit_searchname'].str.lower() == 'uur'].copy()
    print(f"- Records met uren: {len(df_projectlines_uren)}")
    
    if len(df_projectlines_uren) > 0:
        total_amount = df_projectlines_uren['amount'].sum()
        total_amountwritten = df_projectlines_uren['amountwritten'].sum()
        print(f"- Totaal amount: {total_amount:,.2f} uren")
        print(f"- Totaal amountwritten: {total_amountwritten:,.2f} uren")
        
        # Per project breakdown
        print(f"\n📋 Per project (projectlines):")
        projectlines_per_project = df_projectlines_uren.groupby('offerprojectbase_id').agg({
            'amount': 'sum',
            'amountwritten': 'sum'
        }).reset_index()
        
        projects_keepers = df_projects[df_projects['company_id'] == keepers_id]
        projectlines_per_project = projectlines_per_project.merge(
            projects_keepers[['id', 'name', 'archived']], 
            left_on='offerprojectbase_id', 
            right_on='id', 
            how='left'
        )
        
        for _, row in projectlines_per_project.iterrows():
            project_name = row['name'] if pd.notna(row['name']) else 'Onbekend project'
            archived = " (Gearchiveerd)" if row['archived'] else ""
            print(f"  - {project_name}{archived} (ID: {row['offerprojectbase_id']}):")
            print(f"    amount: {row['amount']:,.2f} uren")
            print(f"    amountwritten: {row['amountwritten']:,.2f} uren")
    
    # Filter urenregistratie voor Keepershandschoenen
    df_urenregistratie_keepers = df_urenregistratie.merge(
        df_projects[df_projects['company_id'] == keepers_id], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="inner"
    )
    
    print(f"\n🔍 Urenregistratie voor {keepers_name}:")
    print(f"- Totaal records: {len(df_urenregistratie_keepers)}")
    
    # Filter op gefiatteerde uren
    df_uren_filtered = df_urenregistratie_keepers[df_urenregistratie_keepers['status_searchname'] == 'Gefiatteerd'].copy()
    print(f"- Gefiatteerde records: {len(df_uren_filtered)}")
    
    if len(df_uren_filtered) > 0:
        df_uren_filtered['amount'] = pd.to_numeric(df_uren_filtered['amount'], errors='coerce')
        total_uren_urenregistratie = df_uren_filtered['amount'].sum()
        print(f"- Totaal uren in urenregistratie: {total_uren_urenregistratie:,.2f}")
        
        # Per project breakdown
        print(f"\n📋 Per project (urenregistratie):")
        uren_per_project = df_uren_filtered.groupby('offerprojectbase_id')['amount'].sum().reset_index()
        uren_per_project = uren_per_project.merge(
            projects_keepers[['id', 'name', 'archived']], 
            left_on='offerprojectbase_id', 
            right_on='id', 
            how='left'
        )
        
        for _, row in uren_per_project.iterrows():
            project_name = row['name'] if pd.notna(row['name']) else 'Onbekend project'
            archived = " (Gearchiveerd)" if row['archived'] else ""
            print(f"  - {project_name}{archived} (ID: {row['offerprojectbase_id']}): {row['amount']:,.2f} uren")
    
    # Vergelijking
    print(f"\n📊 VERGELIJKING:")
    if len(df_projectlines_uren) > 0 and len(df_uren_filtered) > 0:
        verschil_amount = total_uren_urenregistratie - total_amount
        verschil_amountwritten = total_uren_urenregistratie - total_amountwritten
        
        print(f"- Urenregistratie: {total_uren_urenregistratie:,.2f} uren")
        print(f"- Projectlines amount: {total_amount:,.2f} uren")
        print(f"- Projectlines amountwritten: {total_amountwritten:,.2f} uren")
        print(f"- Verschil (amount): {verschil_amount:+,.2f} uren")
        print(f"- Verschil (amountwritten): {verschil_amountwritten:+,.2f} uren")
        
        if abs(verschil_amount) < abs(verschil_amountwritten):
            print("✅ amount komt dichter bij urenregistratie!")
        elif abs(verschil_amountwritten) < abs(verschil_amount):
            print("✅ amountwritten komt dichter bij urenregistratie!")
        else:
            print("🤔 Beide kolommen zijn even ver van urenregistratie")
    else:
        print("❌ Geen data gevonden in een van beide bronnen")

if __name__ == "__main__":
    analyze_keepershandschoenen()

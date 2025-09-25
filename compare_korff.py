#!/usr/bin/env python3
"""
Script om specifiek Korff Dakwerken B.V. te vergelijken tussen urenregistratie en projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df

def compare_korff_dakwerken():
    """Vergelijk uren van Korff Dakwerken B.V. tussen beide bronnen"""
    
    print("🏢 Korff Dakwerken B.V. - Uren vergelijking")
    print("=" * 50)
    
    # Load companies data
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    # Find Korff Dakwerken B.V.
    korff_company = df_companies[df_companies['companyname'].str.contains('Korff', case=False, na=False)]
    if len(korff_company) == 0:
        print("❌ Korff Dakwerken B.V. niet gevonden!")
        return
    
    korff_id = korff_company.iloc[0]['id']
    korff_name = korff_company.iloc[0]['companyname']
    
    print(f"✅ Gevonden: {korff_name} (ID: {korff_id})")
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    # Load urenregistratie data
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    # Load projects for mapping
    df_projects = load_data_df("projects", columns=["id", "company_id", "name"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"\n📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie)} records")
    print(f"- Projects: {len(df_projects)} records")
    
    # Filter projectlines voor Korff Dakwerken
    df_projectlines_korff = df_projectlines[df_projectlines['bedrijf_id'] == korff_id].copy()
    df_projectlines_korff['amountwritten'] = pd.to_numeric(df_projectlines_korff['amountwritten'], errors='coerce')
    
    print(f"\n🔍 Projectlines voor {korff_name}:")
    print(f"- Totaal records: {len(df_projectlines_korff)}")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines_korff[df_projectlines_korff['unit_searchname'].str.lower() == 'uur'].copy()
    print(f"- Records met uren: {len(df_projectlines_uren)}")
    
    if len(df_projectlines_uren) > 0:
        total_uren_projectlines = df_projectlines_uren['amountwritten'].sum()
        print(f"- Totaal uren in projectlines: {total_uren_projectlines:,.2f}")
        
        # Per project breakdown
        print(f"\n📋 Per project (projectlines):")
        projectlines_per_project = df_projectlines_uren.groupby('offerprojectbase_id')['amountwritten'].sum().reset_index()
        projectlines_per_project = projectlines_per_project.merge(
            df_projects[['id', 'name']], 
            left_on='offerprojectbase_id', 
            right_on='id', 
            how='left'
        )
        projectlines_per_project = projectlines_per_project.sort_values('amountwritten', ascending=False)
        
        for _, row in projectlines_per_project.iterrows():
            project_name = row['name'] if pd.notna(row['name']) else 'Onbekend project'
            print(f"  - {project_name} (ID: {row['offerprojectbase_id']}): {row['amountwritten']:,.2f} uren")
    
    # Filter urenregistratie voor Korff Dakwerken
    df_urenregistratie_korff = df_urenregistratie.merge(
        df_projects[df_projects['company_id'] == korff_id], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="inner"
    )
    
    print(f"\n🔍 Urenregistratie voor {korff_name}:")
    print(f"- Totaal records: {len(df_urenregistratie_korff)}")
    
    # Filter op gefiatteerde uren
    df_uren_filtered = df_urenregistratie_korff[df_urenregistratie_korff['status_searchname'] == 'Gefiatteerd'].copy()
    print(f"- Gefiatteerde records: {len(df_uren_filtered)}")
    
    if len(df_uren_filtered) > 0:
        df_uren_filtered['amount'] = pd.to_numeric(df_uren_filtered['amount'], errors='coerce')
        total_uren_urenregistratie = df_uren_filtered['amount'].sum()
        print(f"- Totaal uren in urenregistratie: {total_uren_urenregistratie:,.2f}")
        
        # Per project breakdown
        print(f"\n📋 Per project (urenregistratie):")
        uren_per_project = df_uren_filtered.groupby('offerprojectbase_id')['amount'].sum().reset_index()
        uren_per_project = uren_per_project.merge(
            df_projects[['id', 'name']], 
            left_on='offerprojectbase_id', 
            right_on='id', 
            how='left'
        )
        uren_per_project = uren_per_project.sort_values('amount', ascending=False)
        
        for _, row in uren_per_project.iterrows():
            project_name = row['name'] if pd.notna(row['name']) else 'Onbekend project'
            print(f"  - {project_name} (ID: {row['offerprojectbase_id']}): {row['amount']:,.2f} uren")
    
    # Vergelijking
    print(f"\n📊 VERGELIJKING:")
    if len(df_projectlines_uren) > 0 and len(df_uren_filtered) > 0:
        verschil = total_uren_urenregistratie - total_uren_projectlines
        verschil_pct = (verschil / total_uren_projectlines) * 100 if total_uren_projectlines > 0 else 0
        
        print(f"- Projectlines: {total_uren_projectlines:,.2f} uren")
        print(f"- Urenregistratie: {total_uren_urenregistratie:,.2f} uren")
        print(f"- Verschil: {verschil:,.2f} uren ({verschil_pct:+.1f}%)")
        
        if abs(verschil) < 1:
            print("✅ Uren komen vrijwel overeen!")
        elif verschil > 0:
            print("⚠️ Urenregistratie heeft meer uren dan projectlines")
        else:
            print("⚠️ Projectlines heeft meer uren dan urenregistratie")
    else:
        print("❌ Geen data gevonden in een van beide bronnen")

if __name__ == "__main__":
    compare_korff_dakwerken()

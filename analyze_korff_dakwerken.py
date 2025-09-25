#!/usr/bin/env python3
"""
Script om Korff Dakwerken te analyseren met de nieuwe app logica
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def analyze_korff_dakwerken():
    """Analyseer Korff Dakwerken met de nieuwe app logica"""
    
    print("🏢 Analyse: Korff Dakwerken met nieuwe app logica")
    print("=" * 60)
    
    # Load data
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "createdon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    # Zoek Korff Dakwerken
    korff_companies = df_companies[df_companies['companyname'].str.contains('Korff', case=False, na=False)]
    print(f"🏢 Korff bedrijven gevonden: {len(korff_companies)}")
    for idx, row in korff_companies.iterrows():
        print(f"  - ID {row['id']}: {row['companyname']}")
    
    if len(korff_companies) == 0:
        print("❌ Geen Korff bedrijven gevonden!")
        return
    
    # Gebruik de eerste Korff bedrijf
    korff_id = korff_companies.iloc[0]['id']
    korff_name = korff_companies.iloc[0]['companyname']
    print(f"\n🎯 Analyseren: {korff_name} (ID: {korff_id})")
    
    # Filter projectlines voor Korff
    df_korff_projectlines = df_projectlines[df_projectlines["bedrijf_id"] == korff_id].copy()
    print(f"📊 Projectlines voor {korff_name}: {len(df_korff_projectlines)} records")
    
    # Filter op uren
    df_korff_uren = df_korff_projectlines[df_korff_projectlines["unit_searchname"] == "uur"].copy()
    print(f"⏰ Projectlines uren: {len(df_korff_uren)} records")
    
    # Converteer amountwritten naar numeriek
    df_korff_uren["amountwritten"] = pd.to_numeric(df_korff_uren["amountwritten"], errors="coerce")
    
    # Totaal uren
    total_uren = df_korff_uren["amountwritten"].sum()
    print(f"📊 Totaal uren (amountwritten): {total_uren:,.2f}")
    
    # Per project
    df_korff_per_project = df_korff_uren.groupby("offerprojectbase_id")["amountwritten"].sum().reset_index()
    df_korff_per_project = df_korff_per_project.sort_values("amountwritten", ascending=False)
    
    print(f"\n📋 Projecten voor {korff_name}:")
    for idx, row in df_korff_per_project.iterrows():
        project_id = row['offerprojectbase_id']
        project_name = df_projects[df_projects['id'] == project_id]['name'].iloc[0] if len(df_projects[df_projects['id'] == project_id]) > 0 else f"Project {project_id}"
        project_archived = df_projects[df_projects['id'] == project_id]['archived'].iloc[0] if len(df_projects[df_projects['id'] == project_id]) > 0 else "Onbekend"
        print(f"  - {project_name} (ID: {project_id}, Archived: {project_archived}): {row['amountwritten']:,.2f} uren")
    
    # Datum analyse
    print(f"\n📅 DATUM ANALYSE:")
    df_korff_uren['createdon_date'] = pd.to_datetime(df_korff_uren['createdon_date'], errors='coerce')
    records_with_date = df_korff_uren['createdon_date'].notna().sum()
    records_without_date = df_korff_uren['createdon_date'].isna().sum()
    print(f"- Records met createdon_date: {records_with_date}")
    print(f"- Records zonder createdon_date: {records_without_date}")
    
    if records_with_date > 0:
        print(f"- Min date: {df_korff_uren['createdon_date'].min()}")
        print(f"- Max date: {df_korff_uren['createdon_date'].max()}")
    
    # Simuleer app filtering (2020-2025)
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    print(f"\n🔍 APP FILTERING SIMULATIE ({start_date.strftime('%Y-%m-%d')} tot {end_date.strftime('%Y-%m-%d')}):")
    
    if records_with_date > 0:
        # Records met datum filteren
        df_korff_with_date = df_korff_uren[
            (df_korff_uren['createdon_date'].notna()) &
            (df_korff_uren['createdon_date'] >= start_date) &
            (df_korff_uren['createdon_date'] <= end_date)
        ]
        # Records zonder datum toevoegen
        df_korff_without_date = df_korff_uren[df_korff_uren['createdon_date'].isna()]
        # Combineer
        df_korff_filtered = pd.concat([df_korff_with_date, df_korff_without_date], ignore_index=True)
        
        print(f"- Records met datum in periode: {len(df_korff_with_date)}")
        print(f"- Records zonder datum (toegevoegd): {len(df_korff_without_date)}")
        print(f"- Totaal na filtering: {len(df_korff_filtered)}")
        
        total_filtered = df_korff_filtered["amountwritten"].sum()
        print(f"- Totaal uren na filtering: {total_filtered:,.2f}")
    else:
        print(f"- Geen datum filtering mogelijk, alle records gebruikt")
        print(f"- Totaal uren: {total_uren:,.2f}")
    
    # Vergelijk met oude urenregistratie data
    print(f"\n🔄 VERGELIJKING MET OUDE DATA:")
    print(f"- Nieuwe app (projectlines): {total_uren:,.2f} uren")
    print(f"- Oude app (urenregistratie): 272.05 uren")
    print(f"- Verschil: {total_uren - 272.05:+,.2f} uren")
    print(f"- Verbetering: {(total_uren / 272.05 - 1) * 100:+.1f}%")

if __name__ == "__main__":
    analyze_korff_dakwerken()

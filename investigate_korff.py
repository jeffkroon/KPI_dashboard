#!/usr/bin/env python3
"""
Script om te onderzoeken waarom er een verschil is tussen projectlines en urenregistratie voor Korff Dakwerken
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_korff_difference():
    """Onderzoek het verschil tussen projectlines en urenregistratie voor Korff"""
    
    print("🔍 Onderzoek: Waarom verschil tussen projectlines en urenregistratie?")
    print("=" * 70)
    
    korff_id = 95837  # Korff Dakwerken Volendam B.V.
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    # Filter voor Korff
    df_projectlines_korff = df_projectlines[
        (df_projectlines['bedrijf_id'] == korff_id) & 
        (df_projectlines['unit_searchname'] == 'uur')
    ].copy()
    
    df_projects_korff = df_projects[df_projects['company_id'] == korff_id]
    
    df_urenregistratie_korff = df_urenregistratie.merge(
        df_projects_korff, 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="inner"
    )
    
    print(f"📊 Data voor Korff Dakwerken:")
    print(f"- Projectlines met uren: {len(df_projectlines_korff)} records")
    print(f"- Projects: {len(df_projects_korff)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie_korff)} records")
    
    # Per project vergelijking
    print(f"\n🔍 Per project vergelijking:")
    
    # Projectlines per project
    df_projectlines_korff['amountwritten'] = pd.to_numeric(df_projectlines_korff['amountwritten'], errors='coerce')
    projectlines_per_project = df_projectlines_korff.groupby('offerprojectbase_id')['amountwritten'].sum().reset_index()
    projectlines_per_project.columns = ['project_id', 'projectlines_uren']
    
    # Urenregistratie per project (alleen gefiatteerd)
    df_urenregistratie_korff['amount'] = pd.to_numeric(df_urenregistratie_korff['amount'], errors='coerce')
    df_urenregistratie_gefiatteerd = df_urenregistratie_korff[df_urenregistratie_korff['status_searchname'] == 'Gefiatteerd']
    uren_per_project = df_urenregistratie_gefiatteerd.groupby('offerprojectbase_id')['amount'].sum().reset_index()
    uren_per_project.columns = ['project_id', 'urenregistratie_uren']
    
    # Merge beide
    comparison = projectlines_per_project.merge(uren_per_project, on='project_id', how='outer')
    comparison = comparison.merge(df_projects_korff[['id', 'name', 'archived']], left_on='project_id', right_on='id', how='left')
    
    comparison['projectlines_uren'] = comparison['projectlines_uren'].fillna(0)
    comparison['urenregistratie_uren'] = comparison['urenregistratie_uren'].fillna(0)
    comparison['verschil'] = comparison['urenregistratie_uren'] - comparison['projectlines_uren']
    
    print(f"\n📋 Per project breakdown:")
    for _, row in comparison.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"\n🏢 {project_name}{archived} (ID: {row['project_id']}):")
        print(f"   - Projectlines: {row['projectlines_uren']:,.2f} uren")
        print(f"   - Urenregistratie: {row['urenregistratie_uren']:,.2f} uren")
        print(f"   - Verschil: {row['verschil']:+,.2f} uren")
        
        if row['verschil'] > 0:
            print(f"   ⚠️ Overschrijding: {row['verschil']:,.2f} uren meer gewerkt dan gepland")
        elif row['verschil'] < 0:
            print(f"   📝 Nog te werken: {abs(row['verschil']):,.2f} uren")
        else:
            print(f"   ✅ Exact overeenkomst")
    
    # Totaal verschil
    total_projectlines = comparison['projectlines_uren'].sum()
    total_urenregistratie = comparison['urenregistratie_uren'].sum()
    total_verschil = total_urenregistratie - total_projectlines
    
    print(f"\n📊 TOTAAL:")
    print(f"- Projectlines: {total_projectlines:,.2f} uren")
    print(f"- Urenregistratie: {total_urenregistratie:,.2f} uren")
    print(f"- Verschil: {total_verschil:+,.2f} uren")
    
    # Analyse van mogelijke oorzaken
    print(f"\n🔍 MOGELIJKE OORZAKEN:")
    
    # 1. Gearchiveerde projecten
    archived_projects = comparison[comparison['archived'] == True]
    if len(archived_projects) > 0:
        print(f"1. Gearchiveerde projecten: {len(archived_projects)} projecten")
        archived_projectlines = archived_projects['projectlines_uren'].sum()
        archived_urenregistratie = archived_projects['urenregistratie_uren'].sum()
        print(f"   - Projectlines: {archived_projectlines:,.2f} uren")
        print(f"   - Urenregistratie: {archived_urenregistratie:,.2f} uren")
    
    # 2. Projecten met alleen projectlines (geen urenregistratie)
    only_projectlines = comparison[comparison['urenregistratie_uren'] == 0]
    if len(only_projectlines) > 0:
        print(f"2. Projecten met alleen projectlines: {len(only_projectlines)} projecten")
        only_projectlines_total = only_projectlines['projectlines_uren'].sum()
        print(f"   - Totaal uren: {only_projectlines_total:,.2f}")
    
    # 3. Projecten met alleen urenregistratie (geen projectlines)
    only_urenregistratie = comparison[comparison['projectlines_uren'] == 0]
    if len(only_urenregistratie) > 0:
        print(f"3. Projecten met alleen urenregistratie: {len(only_urenregistratie)} projecten")
        only_urenregistratie_total = only_urenregistratie['urenregistratie_uren'].sum()
        print(f"   - Totaal uren: {only_urenregistratie_total:,.2f}")
    
    # 4. Overschrijdingen
    overschrijdingen = comparison[comparison['verschil'] > 0]
    if len(overschrijdingen) > 0:
        print(f"4. Overschrijdingen: {len(overschrijdingen)} projecten")
        overschrijding_total = overschrijdingen['verschil'].sum()
        print(f"   - Totaal overschrijding: {overschrijding_total:,.2f} uren")

if __name__ == "__main__":
    investigate_korff_difference()

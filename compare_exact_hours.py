#!/usr/bin/env python3
"""
Script om precies te zien welke uren overeenkomen tussen urenregistratie en projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df

def compare_exact_hours():
    """Vergelijk exact welke uren overeenkomen en welke missen"""
    
    print("🔍 Exacte vergelijking: welke uren komen overeen en welke missen?")
    print("=" * 70)
    
    korff_id = 95837  # Korff Dakwerken Volendam B.V.
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amountwritten", "unit_searchname", "hidefortimewriting"])
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
    
    print(f"📊 Data voor Korff:")
    print(f"- Projectlines: {len(df_projectlines_korff)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie_korff)} records")
    print(f"- Projects: {len(df_projects_korff)} records")
    
    # Per project vergelijking
    print(f"\n🔍 PER PROJECT VERGELIJKING:")
    
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
    
    # Categoriseer projecten
    exact_match = comparison[comparison['verschil'] == 0]
    overschrijding = comparison[comparison['verschil'] > 0]
    onderuitvoering = comparison[comparison['verschil'] < 0]
    only_projectlines = comparison[comparison['urenregistratie_uren'] == 0]
    only_urenregistratie = comparison[comparison['projectlines_uren'] == 0]
    
    print(f"\n📊 CATEGORIEËN:")
    print(f"✅ Exact overeenkomst: {len(exact_match)} projecten")
    print(f"⚠️ Overschrijding (meer gewerkt dan gepland): {len(overschrijding)} projecten")
    print(f"📝 Onderuitvoering (minder gewerkt dan gepland): {len(onderuitvoering)} projecten")
    print(f"🔵 Alleen projectlines (nog niet gestart): {len(only_projectlines)} projecten")
    print(f"🔴 Alleen urenregistratie (geen projectlines): {len(only_urenregistratie)} projecten")
    
    # Details per categorie
    print(f"\n✅ EXACT OVEREENKOMST ({len(exact_match)} projecten):")
    for _, row in exact_match.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"  - {project_name}{archived}: {row['projectlines_uren']:,.2f} uren")
    
    print(f"\n⚠️ OVERSCHRIJDING ({len(overschrijding)} projecten):")
    for _, row in overschrijding.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"  - {project_name}{archived}: {row['projectlines_uren']:,.2f} → {row['urenregistratie_uren']:,.2f} (+{row['verschil']:,.2f})")
    
    print(f"\n📝 ONDERUITVOERING ({len(onderuitvoering)} projecten):")
    for _, row in onderuitvoering.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"  - {project_name}{archived}: {row['projectlines_uren']:,.2f} → {row['urenregistratie_uren']:,.2f} ({row['verschil']:,.2f})")
    
    print(f"\n🔵 ALLEEN PROJECTLINES ({len(only_projectlines)} projecten):")
    for _, row in only_projectlines.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"  - {project_name}{archived}: {row['projectlines_uren']:,.2f} uren (nog niet gestart)")
    
    print(f"\n🔴 ALLEEN URENREGISTRATIE ({len(only_urenregistratie)} projecten):")
    for _, row in only_urenregistratie.iterrows():
        project_name = row['name'] if pd.notna(row['name']) else 'Onbekend'
        archived = " (Gearchiveerd)" if row['archived'] else ""
        print(f"  - {project_name}{archived}: {row['urenregistratie_uren']:,.2f} uren (geen projectlines)")
    
    # Totaal overzicht
    print(f"\n📊 TOTAAL OVERZICHT:")
    total_projectlines = comparison['projectlines_uren'].sum()
    total_urenregistratie = comparison['urenregistratie_uren'].sum()
    total_verschil = total_urenregistratie - total_projectlines
    
    print(f"- Projectlines totaal: {total_projectlines:,.2f} uren")
    print(f"- Urenregistratie totaal: {total_urenregistratie:,.2f} uren")
    print(f"- Totaal verschil: {total_verschil:+,.2f} uren")
    
    # Breakdown van het verschil
    overschrijding_total = overschrijding['verschil'].sum()
    onderuitvoering_total = onderuitvoering['verschil'].sum()
    only_projectlines_total = only_projectlines['projectlines_uren'].sum()
    only_urenregistratie_total = only_urenregistratie['urenregistratie_uren'].sum()
    
    print(f"\n🔍 BREAKDOWN VAN HET VERSCHIL:")
    print(f"- Overschrijdingen: +{overschrijding_total:,.2f} uren")
    print(f"- Onderuitvoeringen: {onderuitvoering_total:,.2f} uren")
    print(f"- Alleen projectlines: -{only_projectlines_total:,.2f} uren")
    print(f"- Alleen urenregistratie: +{only_urenregistratie_total:,.2f} uren")
    print(f"- Totaal: {overschrijding_total + onderuitvoering_total + only_urenregistratie_total - only_projectlines_total:+,.2f} uren")

if __name__ == "__main__":
    compare_exact_hours()

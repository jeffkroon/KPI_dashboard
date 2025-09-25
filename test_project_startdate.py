#!/usr/bin/env python3
"""
Script om project startdate te gebruiken voor datum filtering
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def test_project_startdate():
    """Test project startdate voor datum filtering"""
    
    print("🔍 Test: Project startdate voor datum filtering")
    print("=" * 50)
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived", "startdate_date"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Projects: {len(df_projects)} records")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines[df_projectlines["unit_searchname"] == "uur"].copy()
    print(f"- Projectlines uren: {len(df_projectlines_uren)} records")
    
    # Analyseer startdate_date eerst
    df_projects['startdate_date'] = pd.to_datetime(df_projects['startdate_date'], errors='coerce')
    
    # Merge projectlines met projects
    df_projectlines_with_project = df_projectlines_uren.merge(
        df_projects[["id", "startdate_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    print(f"- Projectlines met project data: {len(df_projectlines_with_project)} records")
    projects_with_startdate = df_projects['startdate_date'].notna().sum()
    print(f"- Projects met startdate_date: {projects_with_startdate} ({projects_with_startdate/len(df_projects)*100:.1f}%)")
    
    if projects_with_startdate > 0:
        print(f"- Min startdate: {df_projects['startdate_date'].min()}")
        print(f"- Max startdate: {df_projects['startdate_date'].max()}")
    
    # Hoeveel projectlines kunnen we koppelen?
    df_projectlines_with_startdate = df_projectlines_with_project[df_projectlines_with_project['startdate_date'].notna()]
    print(f"- Projectlines met startdate_date: {len(df_projectlines_with_startdate)} ({len(df_projectlines_with_startdate)/len(df_projectlines_uren)*100:.1f}%)")
    
    # Test filtering op periode
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    print(f"\n🔍 FILTERING TEST ({start_date.strftime('%Y-%m-%d')} tot {end_date.strftime('%Y-%m-%d')}):")
    
    # Filter op periode
    df_filtered = df_projectlines_with_startdate[
        (df_projectlines_with_startdate['startdate_date'] >= start_date) &
        (df_projectlines_with_startdate['startdate_date'] <= end_date)
    ]
    
    print(f"- Projectlines in periode: {len(df_filtered)}")
    
    # Bereken totaal uren
    df_filtered["amountwritten"] = pd.to_numeric(df_filtered["amountwritten"], errors="coerce")
    total_uren = df_filtered["amountwritten"].sum()
    print(f"- Totaal uren in periode: {total_uren:,.2f}")
    
    # Test voor Korff Dakwerken
    print(f"\n🏢 KORFF DAKWERKEN TEST:")
    korff_projectlines = df_projectlines_with_project[df_projectlines_with_project["bedrijf_id"] == 95837].copy()
    print(f"- Korff projectlines: {len(korff_projectlines)} records")
    
    # Toon projecten met startdate
    korff_with_startdate = korff_projectlines[korff_projectlines['startdate_date'].notna()]
    print(f"- Korff projecten met startdate: {len(korff_with_startdate)}")
    
    for idx, row in korff_with_startdate.iterrows():
        project_name = df_projects[df_projects['id'] == row['offerprojectbase_id']]['name'].iloc[0] if len(df_projects[df_projects['id'] == row['offerprojectbase_id']]) > 0 else f"Project {row['offerprojectbase_id']}"
        print(f"  - {project_name}: {row['startdate_date']} (amountwritten: {row['amountwritten']})")
    
    # Aanbeveling
    print(f"\n💡 AANBEVELING:")
    print(f"- Gebruik project startdate_date voor datum filtering")
    print(f"- Coverage: {len(df_projectlines_with_startdate)/len(df_projectlines_uren)*100:.1f}% van projectlines")
    print(f"- Fallback: Toon alle projectlines als geen startdate beschikbaar")
    print(f"- Dit geeft veel betere datum filtering dan projectlines createdon_date")

if __name__ == "__main__":
    test_project_startdate()

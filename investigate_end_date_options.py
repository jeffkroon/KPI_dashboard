#!/usr/bin/env python3
"""
Script om end date opties te onderzoeken voor projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def investigate_end_date_options():
    """Onderzoek end date opties voor projectlines"""
    
    print("🔍 Onderzoek: End date opties voor projectlines")
    print("=" * 50)
    
    # Load data
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived", "startdate_date", "deadline_date", "enddate_date", "updatedon_date"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    df_invoices = load_data_df("invoices", columns=["id", "company_id", "date_date", "reportdate_date", "status_searchname"])
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projects: {len(df_projects)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    
    # === OPTIE 1: PROJECT ENDDATE ===
    print(f"\n🔍 OPTIE 1: Project enddate_date")
    print("-" * 40)
    
    df_projects['enddate_date'] = pd.to_datetime(df_projects['enddate_date'], errors='coerce')
    projects_with_enddate = df_projects['enddate_date'].notna().sum()
    print(f"- Projects met enddate_date: {projects_with_enddate} ({projects_with_enddate/len(df_projects)*100:.1f}%)")
    
    if projects_with_enddate > 0:
        print(f"- Min enddate: {df_projects['enddate_date'].min()}")
        print(f"- Max enddate: {df_projects['enddate_date'].max()}")
    
    # === OPTIE 2: PROJECT DEADLINE ===
    print(f"\n🔍 OPTIE 2: Project deadline_date")
    print("-" * 40)
    
    df_projects['deadline_date'] = pd.to_datetime(df_projects['deadline_date'], errors='coerce')
    projects_with_deadline = df_projects['deadline_date'].notna().sum()
    print(f"- Projects met deadline_date: {projects_with_deadline} ({projects_with_deadline/len(df_projects)*100:.1f}%)")
    
    if projects_with_deadline > 0:
        print(f"- Min deadline: {df_projects['deadline_date'].min()}")
        print(f"- Max deadline: {df_projects['deadline_date'].max()}")
    
    # === OPTIE 3: PROJECT UPDATEDON ===
    print(f"\n🔍 OPTIE 3: Project updatedon_date")
    print("-" * 40)
    
    df_projects['updatedon_date'] = pd.to_datetime(df_projects['updatedon_date'], errors='coerce')
    projects_with_updatedon = df_projects['updatedon_date'].notna().sum()
    print(f"- Projects met updatedon_date: {projects_with_updatedon} ({projects_with_updatedon/len(df_projects)*100:.1f}%)")
    
    if projects_with_updatedon > 0:
        print(f"- Min updatedon: {df_projects['updatedon_date'].min()}")
        print(f"- Max updatedon: {df_projects['updatedon_date'].max()}")
    
    # === OPTIE 4: INVOICE DATUM ===
    print(f"\n🔍 OPTIE 4: Invoice datum")
    print("-" * 40)
    
    df_invoices['reportdate_date'] = pd.to_datetime(df_invoices['reportdate_date'], errors='coerce')
    invoices_with_reportdate = df_invoices['reportdate_date'].notna().sum()
    print(f"- Invoices met reportdate_date: {invoices_with_reportdate} ({invoices_with_reportdate/len(df_invoices)*100:.1f}%)")
    
    if invoices_with_reportdate > 0:
        print(f"- Min reportdate: {df_invoices['reportdate_date'].min()}")
        print(f"- Max reportdate: {df_invoices['reportdate_date'].max()}")
    
    # === VERGELIJKING ===
    print(f"\n🔍 VERGELIJKING:")
    print(f"- enddate_date: {projects_with_enddate} projects ({projects_with_enddate/len(df_projects)*100:.1f}%)")
    print(f"- deadline_date: {projects_with_deadline} projects ({projects_with_deadline/len(df_projects)*100:.1f}%)")
    print(f"- updatedon_date: {projects_with_updatedon} projects ({projects_with_updatedon/len(df_projects)*100:.1f}%)")
    print(f"- invoice reportdate: {invoices_with_reportdate} invoices ({invoices_with_reportdate/len(df_invoices)*100:.1f}%)")
    
    # === TEST VOOR KORFF ===
    print(f"\n🏢 KORFF DAKWERKEN TEST:")
    
    # Zoek Korff projecten
    korff_projects = df_projects[df_projects['company_id'] == 95837].copy()
    print(f"- Korff projecten: {len(korff_projects)}")
    
    for idx, row in korff_projects.iterrows():
        print(f"  - {row['name']}:")
        print(f"    - Startdate: {row['startdate_date']}")
        print(f"    - Enddate: {row['enddate_date']}")
        print(f"    - Deadline: {row['deadline_date']}")
        print(f"    - Updatedon: {row['updatedon_date']}")
    
    # === AANBEVELING ===
    print(f"\n💡 AANBEVELINGEN:")
    print(f"1. **Project enddate_date**: {projects_with_enddate} projects ({projects_with_enddate/len(df_projects)*100:.1f}%)")
    print(f"2. **Project deadline_date**: {projects_with_deadline} projects ({projects_with_deadline/len(df_projects)*100:.1f}%)")
    print(f"3. **Project updatedon_date**: {projects_with_updatedon} projects ({projects_with_updatedon/len(df_projects)*100:.1f}%)")
    print(f"4. **Invoice reportdate_date**: {invoices_with_reportdate} invoices ({invoices_with_reportdate/len(df_invoices)*100:.1f}%)")
    
    # Beste optie
    best_option = "enddate_date" if projects_with_enddate > projects_with_deadline else "deadline_date"
    print(f"\n🎯 BESTE OPTIE: {best_option}")
    print(f"- Meest logisch: enddate_date (wanneer project eindigde)")
    print(f"- Fallback: deadline_date (wanneer project moest eindigen)")
    print(f"- Fallback: updatedon_date (laatste update)")

if __name__ == "__main__":
    investigate_end_date_options()

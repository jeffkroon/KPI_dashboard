#!/usr/bin/env python3
"""
Script om de beste datum koppeling te vinden voor projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def find_best_date_linking():
    """Vind de beste datum koppeling voor projectlines"""
    
    print("🔍 Onderzoek: Beste datum koppeling voor projectlines")
    print("=" * 60)
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "createdon_date", "updatedon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived", "startdate_date", "deadline_date", "enddate_date", "updatedon_date"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    df_invoices = load_data_df("invoices", columns=["id", "company_id", "offerprojectbase_id", "date_date", "reportdate_date", "status_searchname"])
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Projects: {len(df_projects)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines[df_projectlines["unit_searchname"] == "uur"].copy()
    print(f"- Projectlines uren: {len(df_projectlines_uren)} records")
    
    # === OPTIE 1: PROJECT STARTDATE ===
    print(f"\n🔍 OPTIE 1: Project startdate_date")
    print("-" * 40)
    
    # Merge projectlines met projects
    df_projectlines_with_project = df_projectlines_uren.merge(
        df_projects[["id", "startdate_date", "deadline_date", "enddate_date", "updatedon_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    # Analyseer startdate_date
    df_projects['startdate_date'] = pd.to_datetime(df_projects['startdate_date'], errors='coerce')
    projects_with_startdate = df_projects['startdate_date'].notna().sum()
    print(f"- Projects met startdate_date: {projects_with_startdate} ({projects_with_startdate/len(df_projects)*100:.1f}%)")
    
    if projects_with_startdate > 0:
        print(f"- Min startdate: {df_projects['startdate_date'].min()}")
        print(f"- Max startdate: {df_projects['startdate_date'].max()}")
    
    # Hoeveel projectlines kunnen we koppelen?
    df_projectlines_with_startdate = df_projectlines_with_project[df_projectlines_with_project['startdate_date'].notna()]
    print(f"- Projectlines met startdate_date: {len(df_projectlines_with_startdate)} ({len(df_projectlines_with_startdate)/len(df_projectlines_uren)*100:.1f}%)")
    
    # === OPTIE 2: PROJECT UPDATEDON ===
    print(f"\n🔍 OPTIE 2: Project updatedon_date")
    print("-" * 40)
    
    df_projects['updatedon_date'] = pd.to_datetime(df_projects['updatedon_date'], errors='coerce')
    projects_with_updatedon = df_projects['updatedon_date'].notna().sum()
    print(f"- Projects met updatedon_date: {projects_with_updatedon} ({projects_with_updatedon/len(df_projects)*100:.1f}%)")
    
    if projects_with_updatedon > 0:
        print(f"- Min updatedon: {df_projects['updatedon_date'].min()}")
        print(f"- Max updatedon: {df_projects['updatedon_date'].max()}")
    
    df_projectlines_with_updatedon = df_projectlines_with_project[df_projectlines_with_project['updatedon_date'].notna()]
    print(f"- Projectlines met updatedon_date: {len(df_projectlines_with_updatedon)} ({len(df_projectlines_with_updatedon)/len(df_projectlines_uren)*100:.1f}%)")
    
    # === OPTIE 3: INVOICE DATUM ===
    print(f"\n🔍 OPTIE 3: Invoice datum")
    print("-" * 40)
    
    # Merge projectlines met invoices
    df_projectlines_with_invoice = df_projectlines_uren.merge(
        df_invoices[["offerprojectbase_id", "date_date", "reportdate_date"]], 
        on="offerprojectbase_id", 
        how="left"
    )
    
    # Analyseer invoice datum velden
    for col in ["date_date", "reportdate_date"]:
        if col in df_invoices.columns:
            df_invoices[col] = pd.to_datetime(df_invoices[col], errors='coerce')
            invoices_with_date = df_invoices[col].notna().sum()
            print(f"- Invoices met {col}: {invoices_with_date} ({invoices_with_date/len(df_invoices)*100:.1f}%)")
            
            if invoices_with_date > 0:
                print(f"  - Min: {df_invoices[col].min()}")
                print(f"  - Max: {df_invoices[col].max()}")
    
    # Hoeveel projectlines kunnen we koppelen?
    df_projectlines_with_invoice_date = df_projectlines_with_invoice[df_projectlines_with_invoice['reportdate_date'].notna()]
    print(f"- Projectlines met invoice reportdate_date: {len(df_projectlines_with_invoice_date)} ({len(df_projectlines_with_invoice_date)/len(df_projectlines_uren)*100:.1f}%)")
    
    # === OPTIE 4: COMBINATIE ===
    print(f"\n🔍 OPTIE 4: Combinatie (fallback)")
    print("-" * 40)
    
    # Test voor Korff Dakwerken
    korff_projectlines = df_projectlines_uren[df_projectlines_uren["bedrijf_id"] == 95837].copy()
    print(f"📊 Korff projectlines: {len(korff_projectlines)} records")
    
    # Merge met projects
    korff_with_project = korff_projectlines.merge(
        df_projects[["id", "startdate_date", "updatedon_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    print(f"📊 Korff projectlines met project data: {len(korff_with_project)}")
    
    # Toon voorbeelden
    print(f"\n📋 VOORBEELDEN:")
    for idx, row in korff_with_project.head(3).iterrows():
        print(f"Projectline ID {row['id']}:")
        print(f"  - Project startdate: {row['startdate_date']}")
        print(f"  - Project updatedon: {row['updatedon_date']}")
    
    # === AANBEVELING ===
    print(f"\n💡 AANBEVELINGEN:")
    print(f"1. **Project startdate_date**: {len(df_projectlines_with_startdate)} projectlines ({len(df_projectlines_with_startdate)/len(df_projectlines_uren)*100:.1f}%)")
    print(f"2. **Project updatedon_date**: {len(df_projectlines_with_updatedon)} projectlines ({len(df_projectlines_with_updatedon)/len(df_projectlines_uren)*100:.1f}%)")
    print(f"3. **Invoice reportdate_date**: {len(df_projectlines_with_invoice_date)} projectlines ({len(df_projectlines_with_invoice_date)/len(df_projectlines_uren)*100:.1f}%)")
    
    # Beste optie
    best_option = "startdate_date" if len(df_projectlines_with_startdate) > len(df_projectlines_with_updatedon) else "updatedon_date"
    print(f"\n🎯 BESTE OPTIE: {best_option}")
    print(f"- Meest logisch: startdate_date (wanneer werk begon)")
    print(f"- Beste coverage: {len(df_projectlines_with_startdate)} projectlines")
    print(f"- Fallback: updatedon_date voor records zonder startdate")

if __name__ == "__main__":
    find_best_date_linking()

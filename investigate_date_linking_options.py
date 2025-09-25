#!/usr/bin/env python3
"""
Script om datum koppeling opties te onderzoeken voor projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def investigate_date_linking_options():
    """Onderzoek verschillende opties voor datum koppeling"""
    
    print("🔍 Onderzoek: Datum koppeling opties voor projectlines")
    print("=" * 60)
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "createdon_date", "updatedon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived", "createdon_date", "updatedon_date", "startdate_date", "enddate_date"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    df_invoices = load_data_df("invoices", columns=["id", "company_id", "offerprojectbase_id", "date_date", "reportdate_date", "status_searchname"])
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Projects: {len(df_projects)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    
    # === OPTIE 1: PROJECT DATUM ===
    print(f"\n🔍 OPTIE 1: Project datum gebruiken")
    print("-" * 40)
    
    # Merge projectlines met projects
    df_projectlines_with_project = df_projectlines.merge(
        df_projects[["id", "createdon_date", "updatedon_date", "startdate_date", "enddate_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    # Analyseer project datum velden
    for col in ["createdon_date", "updatedon_date", "startdate_date", "enddate_date"]:
        if col in df_projects.columns:
            df_projects[col] = pd.to_datetime(df_projects[col], errors='coerce')
            records_with_date = df_projects[col].notna().sum()
            print(f"- Projects met {col}: {records_with_date} ({records_with_date/len(df_projects)*100:.1f}%)")
            
            if records_with_date > 0:
                print(f"  - Min: {df_projects[col].min()}")
                print(f"  - Max: {df_projects[col].max()}")
    
    # === OPTIE 2: INVOICE DATUM ===
    print(f"\n🔍 OPTIE 2: Invoice datum gebruiken")
    print("-" * 40)
    
    # Merge projectlines met invoices
    df_projectlines_with_invoice = df_projectlines.merge(
        df_invoices[["offerprojectbase_id", "date_date", "reportdate_date"]], 
        on="offerprojectbase_id", 
        how="left"
    )
    
    # Analyseer invoice datum velden
    for col in ["date_date", "reportdate_date"]:
        if col in df_invoices.columns:
            df_invoices[col] = pd.to_datetime(df_invoices[col], errors='coerce')
            records_with_date = df_invoices[col].notna().sum()
            print(f"- Invoices met {col}: {records_with_date} ({records_with_date/len(df_invoices)*100:.1f}%)")
            
            if records_with_date > 0:
                print(f"  - Min: {df_invoices[col].min()}")
                print(f"  - Max: {df_invoices[col].max()}")
    
    # === OPTIE 3: COMBINATIE ===
    print(f"\n🔍 OPTIE 3: Combinatie van datums")
    print("-" * 40)
    
    # Test voor een specifiek bedrijf (Korff)
    korff_projectlines = df_projectlines[df_projectlines["bedrijf_id"] == 95837].copy()
    print(f"📊 Korff projectlines: {len(korff_projectlines)} records")
    
    # Merge met projects
    korff_with_project = korff_projectlines.merge(
        df_projects[["id", "createdon_date", "updatedon_date", "startdate_date", "enddate_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    # Merge met invoices
    korff_with_invoice = korff_projectlines.merge(
        df_invoices[["offerprojectbase_id", "date_date", "reportdate_date"]], 
        on="offerprojectbase_id", 
        how="left"
    )
    
    print(f"📊 Korff projectlines met project data: {len(korff_with_project)}")
    print(f"📊 Korff projectlines met invoice data: {len(korff_with_invoice)}")
    
    # Toon voorbeelden
    print(f"\n📋 VOORBEELDEN:")
    for idx, row in korff_with_project.head(3).iterrows():
        print(f"Projectline ID {row['id']}:")
        print(f"  - Project createdon: {row['createdon_date']}")
        print(f"  - Project updatedon: {row['updatedon_date']}")
        print(f"  - Project startdate: {row['startdate_date']}")
        print(f"  - Project enddate: {row['enddate_date']}")
    
    # === AANBEVELING ===
    print(f"\n💡 AANBEVELINGEN:")
    print(f"1. **Project startdate_date**: Meest logisch voor wanneer werk begon")
    print(f"2. **Project createdon_date**: Wanneer project werd aangemaakt")
    print(f"3. **Invoice reportdate_date**: Wanneer werk werd gefactureerd")
    print(f"4. **Fallback**: Gebruik projectlines createdon_date als backup")
    
    # Test coverage
    print(f"\n🧪 COVERAGE TEST:")
    if 'startdate_date' in df_projects.columns:
        df_projects['startdate_date'] = pd.to_datetime(df_projects['startdate_date'], errors='coerce')
        projects_with_startdate = df_projects['startdate_date'].notna().sum()
        print(f"- Projects met startdate_date: {projects_with_startdate} ({projects_with_startdate/len(df_projects)*100:.1f}%)")
        
        if projects_with_startdate > 0:
            # Hoeveel projectlines kunnen we koppelen?
            df_projectlines_with_startdate = df_projectlines_with_project[df_projectlines_with_project['startdate_date'].notna()]
            print(f"- Projectlines met startdate_date: {len(df_projectlines_with_startdate)} ({len(df_projectlines_with_startdate)/len(df_projectlines)*100:.1f}%)")

if __name__ == "__main__":
    investigate_date_linking_options()

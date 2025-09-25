#!/usr/bin/env python3
"""
Script om te onderzoeken hoe kloppend de data is en hoeveel we missen
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def investigate_data_accuracy():
    """Onderzoek hoe kloppend de data is en hoeveel we missen"""
    
    print("🔍 Onderzoek: Data accuratesse en missing data")
    print("=" * 60)
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived", "startdate_date", "deadline_date", "enddate_date", "updatedon_date"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    df_invoices = load_data_df("invoices", columns=["id", "company_id", "date_date", "reportdate_date", "status_searchname"])
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Projects: {len(df_projects)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines[df_projectlines["unit_searchname"] == "uur"].copy()
    print(f"- Projectlines uren: {len(df_projectlines_uren)} records")
    
    # Converteer datum kolommen
    for col in ["startdate_date", "deadline_date", "enddate_date", "updatedon_date"]:
        df_projects[col] = pd.to_datetime(df_projects[col], errors='coerce')
    
    # Merge projectlines met projects
    df_projectlines_with_project = df_projectlines_uren.merge(
        df_projects[["id", "startdate_date", "deadline_date", "enddate_date", "updatedon_date"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    print(f"- Projectlines met project data: {len(df_projectlines_with_project)} records")
    
    # === ANALYSE PER END DATE OPTIE ===
    print(f"\n🔍 ANALYSE PER END DATE OPTIE:")
    print("-" * 50)
    
    # Optie 1: Alleen enddate_date
    df_with_enddate = df_projectlines_with_project[df_projectlines_with_project['enddate_date'].notna()]
    print(f"1. **Alleen enddate_date**:")
    print(f"   - Records: {len(df_with_enddate)} ({len(df_with_enddate)/len(df_projectlines_with_project)*100:.1f}%)")
    print(f"   - Missing: {len(df_projectlines_with_project) - len(df_with_enddate)} ({100 - len(df_with_enddate)/len(df_projectlines_with_project)*100:.1f}%)")
    
    # Optie 2: enddate_date + deadline_date fallback
    df_with_enddate_or_deadline = df_projectlines_with_project[
        (df_projectlines_with_project['enddate_date'].notna()) |
        (df_projectlines_with_project['deadline_date'].notna())
    ]
    print(f"2. **enddate_date + deadline_date fallback**:")
    print(f"   - Records: {len(df_with_enddate_or_deadline)} ({len(df_with_enddate_or_deadline)/len(df_projectlines_with_project)*100:.1f}%)")
    print(f"   - Missing: {len(df_projectlines_with_project) - len(df_with_enddate_or_deadline)} ({100 - len(df_with_enddate_or_deadline)/len(df_projectlines_with_project)*100:.1f}%)")
    
    # Optie 3: enddate_date + deadline_date + updatedon_date fallback
    df_with_any_enddate = df_projectlines_with_project[
        (df_projectlines_with_project['enddate_date'].notna()) |
        (df_projectlines_with_project['deadline_date'].notna()) |
        (df_projectlines_with_project['updatedon_date'].notna())
    ]
    print(f"3. **enddate_date + deadline_date + updatedon_date fallback**:")
    print(f"   - Records: {len(df_with_any_enddate)} ({len(df_with_any_enddate)/len(df_projectlines_with_project)*100:.1f}%)")
    print(f"   - Missing: {len(df_projectlines_with_project) - len(df_with_any_enddate)} ({100 - len(df_with_any_enddate)/len(df_projectlines_with_project)*100:.1f}%)")
    
    # === DATUM LOGICA TEST ===
    print(f"\n🔍 DATUM LOGICA TEST:")
    print("-" * 30)
    
    # Test voor een specifiek bedrijf (Korff)
    korff_projectlines = df_projectlines_with_project[df_projectlines_with_project["bedrijf_id"] == 95837].copy()
    print(f"📊 Korff projectlines: {len(korff_projectlines)} records")
    
    # Toon datum logica voor Korff projecten
    for idx, row in korff_projectlines.iterrows():
        project_name = df_projects[df_projects['id'] == row['offerprojectbase_id']]['name'].iloc[0] if len(df_projects[df_projects['id'] == row['offerprojectbase_id']]) > 0 else f"Project {row['offerprojectbase_id']}"
        
        # Bepaal end date volgens logica
        if pd.notna(row['enddate_date']):
            end_date = row['enddate_date']
            end_date_source = "enddate_date"
        elif pd.notna(row['deadline_date']):
            end_date = row['deadline_date']
            end_date_source = "deadline_date"
        elif pd.notna(row['updatedon_date']):
            end_date = row['updatedon_date']
            end_date_source = "updatedon_date"
        else:
            end_date = "Geen datum"
            end_date_source = "missing"
        
        print(f"  - {project_name}:")
        print(f"    - Start: {row['startdate_date']}")
        print(f"    - End: {end_date} ({end_date_source})")
        print(f"    - Amountwritten: {row['amountwritten']}")
    
    # === PERIODE FILTERING TEST ===
    print(f"\n🔍 PERIODE FILTERING TEST:")
    print("-" * 30)
    
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    print(f"Periode: {start_date.strftime('%Y-%m-%d')} tot {end_date.strftime('%Y-%m-%d')}")
    
    # Test verschillende end date logica's
    for optie, naam in [
        (df_with_enddate, "Alleen enddate_date"),
        (df_with_enddate_or_deadline, "enddate_date + deadline_date"),
        (df_with_any_enddate, "Alle end date opties")
    ]:
        # Filter op periode
        df_filtered = optie[
            (optie['startdate_date'] >= start_date) &
            (optie['startdate_date'] <= end_date)
        ]
        
        # Bereken totaal uren
        df_filtered["amountwritten"] = pd.to_numeric(df_filtered["amountwritten"], errors="coerce")
        total_uren = df_filtered["amountwritten"].sum()
        
        print(f"- {naam}:")
        print(f"  - Records in periode: {len(df_filtered)}")
        print(f"  - Totaal uren: {total_uren:,.2f}")
    
    # === AANBEVELING ===
    print(f"\n💡 AANBEVELING:")
    print(f"- **Beste optie**: enddate_date + deadline_date + updatedon_date fallback")
    print(f"- **Coverage**: {len(df_with_any_enddate)/len(df_projectlines_with_project)*100:.1f}% van projectlines")
    print(f"- **Missing**: {len(df_projectlines_with_project) - len(df_with_any_enddate)} records ({100 - len(df_with_any_enddate)/len(df_projectlines_with_project)*100:.1f}%)")
    print(f"- **Logica**: Gebruik enddate_date als beschikbaar, anders deadline_date, anders updatedon_date")
    print(f"- **Accuratesse**: enddate_date is meest accuraat, deadline_date is logisch, updatedon_date is fallback")

if __name__ == "__main__":
    investigate_data_accuracy()

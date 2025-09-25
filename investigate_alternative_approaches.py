#!/usr/bin/env python3
"""
Script om alternatieve manieren te onderzoeken voor datum filtering
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def investigate_alternative_approaches():
    """Onderzoek alternatieve manieren voor datum filtering"""
    
    print("🔍 Onderzoek: Alternatieve manieren voor datum filtering")
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
    
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Projects: {len(df_projects)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie)} records")
    
    # Filter op uren
    df_projectlines_uren = df_projectlines[df_projectlines["unit_searchname"] == "uur"].copy()
    print(f"- Projectlines uren: {len(df_projectlines_uren)} records")
    
    # === ALTERNATIEF 1: GEEN DATUM FILTERING ===
    print(f"\n🔍 ALTERNATIEF 1: Geen datum filtering")
    print("-" * 40)
    print(f"- Toon alle projectlines ongeacht datum")
    print(f"- Records: {len(df_projectlines_uren)} (100%)")
    print(f"- Totaal uren: {pd.to_numeric(df_projectlines_uren['amountwritten'], errors='coerce').sum():,.2f}")
    print(f"- Voordeel: Geen missing data")
    print(f"- Nadeel: Geen periode filtering mogelijk")
    
    # === ALTERNATIEF 2: INVOICE DATUM ===
    print(f"\n🔍 ALTERNATIEF 2: Invoice datum")
    print("-" * 40)
    
    # Merge projectlines met invoices (via projects)
    df_projectlines_with_invoice = df_projectlines_uren.merge(
        df_projects[["id", "company_id"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    df_projectlines_with_invoice = df_projectlines_with_invoice.merge(
        df_invoices[["company_id", "reportdate_date"]], 
        on="company_id", 
        how="left"
    )
    
    df_invoices['reportdate_date'] = pd.to_datetime(df_invoices['reportdate_date'], errors='coerce')
    invoices_with_date = df_invoices['reportdate_date'].notna().sum()
    print(f"- Invoices met reportdate_date: {invoices_with_date} ({invoices_with_date/len(df_invoices)*100:.1f}%)")
    
    # Hoeveel projectlines kunnen we koppelen?
    df_projectlines_with_invoice_date = df_projectlines_with_invoice[df_projectlines_with_invoice['reportdate_date'].notna()]
    print(f"- Projectlines met invoice datum: {len(df_projectlines_with_invoice_date)} ({len(df_projectlines_with_invoice_date)/len(df_projectlines_uren)*100:.1f}%)")
    
    if len(df_projectlines_with_invoice_date) > 0:
        total_uren = pd.to_numeric(df_projectlines_with_invoice_date['amountwritten'], errors='coerce').sum()
        print(f"- Totaal uren: {total_uren:,.2f}")
    
    # === ALTERNATIEF 3: URENREGISTRATIE DATUM ===
    print(f"\n🔍 ALTERNATIEF 3: Urenregistratie datum")
    print("-" * 40)
    
    # Merge projectlines met urenregistratie
    df_projectlines_with_uren = df_projectlines_uren.merge(
        df_urenregistratie[["offerprojectbase_id", "date_date"]], 
        on="offerprojectbase_id", 
        how="left"
    )
    
    df_urenregistratie['date_date'] = pd.to_datetime(df_urenregistratie['date_date'], errors='coerce')
    uren_with_date = df_urenregistratie['date_date'].notna().sum()
    print(f"- Urenregistratie met date_date: {uren_with_date} ({uren_with_date/len(df_urenregistratie)*100:.1f}%)")
    
    # Hoeveel projectlines kunnen we koppelen?
    df_projectlines_with_uren_date = df_projectlines_with_uren[df_projectlines_with_uren['date_date'].notna()]
    print(f"- Projectlines met urenregistratie datum: {len(df_projectlines_with_uren_date)} ({len(df_projectlines_with_uren_date)/len(df_projectlines_uren)*100:.1f}%)")
    
    if len(df_projectlines_with_uren_date) > 0:
        total_uren = pd.to_numeric(df_projectlines_with_uren_date['amountwritten'], errors='coerce').sum()
        print(f"- Totaal uren: {total_uren:,.2f}")
    
    # === ALTERNATIEF 4: HYBRIDE AANPAK ===
    print(f"\n🔍 ALTERNATIEF 4: Hybride aanpak")
    print("-" * 40)
    print(f"- Gebruik projectlines voor uren data")
    print(f"- Gebruik urenregistratie voor datum filtering")
    print(f"- Combineer beide bronnen")
    
    # Test voor Korff
    korff_projectlines = df_projectlines_uren[df_projectlines_uren["bedrijf_id"] == 95837].copy()
    korff_urenregistratie = df_urenregistratie.merge(
        df_projects[["id", "company_id"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    korff_urenregistratie = korff_urenregistratie[korff_urenregistratie["company_id"] == 95837]
    
    print(f"- Korff projectlines: {len(korff_projectlines)} records")
    print(f"- Korff urenregistratie: {len(korff_urenregistratie)} records")
    
    # === ALTERNATIEF 5: PROJECT STATUS ===
    print(f"\n🔍 ALTERNATIEF 5: Project status filtering")
    print("-" * 40)
    
    # Analyseer project status
    archived_projects = df_projects[df_projects['archived'] == True]
    active_projects = df_projects[df_projects['archived'] == False]
    
    print(f"- Gearchiveerde projecten: {len(archived_projects)}")
    print(f"- Actieve projecten: {len(active_projects)}")
    
    # Projectlines per status
    df_projectlines_with_status = df_projectlines_uren.merge(
        df_projects[["id", "archived"]], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="left"
    )
    
    archived_projectlines = df_projectlines_with_status[df_projectlines_with_status['archived'] == True]
    active_projectlines = df_projectlines_with_status[df_projectlines_with_status['archived'] == False]
    
    print(f"- Projectlines gearchiveerde projecten: {len(archived_projectlines)}")
    print(f"- Projectlines actieve projecten: {len(active_projectlines)}")
    
    # === ALTERNATIEF 6: BEDRIJF CREATIE DATUM ===
    print(f"\n🔍 ALTERNATIEF 6: Bedrijf creatie datum")
    print("-" * 40)
    
    # Laad companies data
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    print(f"- Companies: {len(df_companies)} records")
    print(f"- Kolommen: {list(df_companies.columns)}")
    
    # === AANBEVELINGEN ===
    print(f"\n💡 AANBEVELINGEN:")
    print(f"1. **Geen datum filtering**: 100% coverage, geen missing data")
    print(f"2. **Invoice datum**: {len(df_projectlines_with_invoice_date)/len(df_projectlines_uren)*100:.1f}% coverage")
    print(f"3. **Urenregistratie datum**: {len(df_projectlines_with_uren_date)/len(df_projectlines_uren)*100:.1f}% coverage")
    print(f"4. **Hybride aanpak**: Combineer projectlines uren met urenregistratie datum")
    print(f"5. **Project status**: Filter op gearchiveerde vs. actieve projecten")
    print(f"6. **Bedrijf creatie**: Filter op wanneer bedrijf werd aangemaakt")
    
    print(f"\n🎯 BESTE ALTERNATIEF:")
    print(f"- **Hybride aanpak**: Gebruik projectlines voor uren, urenregistratie voor datum")
    print(f"- **Voordeel**: Beste van beide werelden")
    print(f"- **Nadeel**: Complexere logica")

if __name__ == "__main__":
    investigate_alternative_approaches()

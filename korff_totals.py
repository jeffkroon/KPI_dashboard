#!/usr/bin/env python3
"""
Script om het totaal van Korff Dakwerken te berekenen uit beide bronnen
"""

import pandas as pd
from utils.data_loaders import load_data_df

def calculate_korff_totals():
    """Bereken totaal van Korff uit beide bronnen"""
    
    print("🏢 Korff Dakwerken Volendam B.V. - Totaal berekening")
    print("=" * 60)
    
    korff_id = 95837  # Korff Dakwerken Volendam B.V.
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    # === PROJECTLINES TOTAAL ===
    print("📊 PROJECTLINES TOTAAL:")
    
    # Filter projectlines voor Korff op uren
    df_projectlines_korff = df_projectlines[
        (df_projectlines['bedrijf_id'] == korff_id) & 
        (df_projectlines['unit_searchname'] == 'uur')
    ].copy()
    
    df_projectlines_korff['amountwritten'] = pd.to_numeric(df_projectlines_korff['amountwritten'], errors='coerce')
    projectlines_total = df_projectlines_korff['amountwritten'].sum()
    
    print(f"✅ Projectlines records: {len(df_projectlines_korff)}")
    print(f"✅ Totaal uren uit projectlines: {projectlines_total:,.2f}")
    
    # === URENREGISTRATIE TOTAAL ===
    print(f"\n📊 URENREGISTRATIE TOTAAL:")
    
    # Filter urenregistratie voor Korff
    df_urenregistratie_korff = df_urenregistratie.merge(
        df_projects[df_projects['company_id'] == korff_id], 
        left_on="offerprojectbase_id", 
        right_on="id", 
        how="inner"
    )
    
    print(f"✅ Urenregistratie records (alle): {len(df_urenregistratie_korff)}")
    
    # Alle uren (niet alleen gefiatteerd)
    df_urenregistratie_korff['amount'] = pd.to_numeric(df_urenregistratie_korff['amount'], errors='coerce')
    urenregistratie_total_all = df_urenregistratie_korff['amount'].sum()
    print(f"✅ Totaal uren uit urenregistratie (alle): {urenregistratie_total_all:,.2f}")
    
    # Alleen gefiatteerde uren
    df_urenregistratie_gefiatteerd = df_urenregistratie_korff[df_urenregistratie_korff['status_searchname'] == 'Gefiatteerd']
    urenregistratie_total_gefiatteerd = df_urenregistratie_gefiatteerd['amount'].sum()
    print(f"✅ Totaal uren uit urenregistratie (gefilterd): {urenregistratie_total_gefiatteerd:,.2f}")
    
    # Status breakdown
    status_breakdown = df_urenregistratie_korff['status_searchname'].value_counts()
    print(f"\n📋 Status breakdown:")
    for status, count in status_breakdown.items():
        status_total = df_urenregistratie_korff[df_urenregistratie_korff['status_searchname'] == status]['amount'].sum()
        print(f"  - {status}: {count} records, {status_total:,.2f} uren")
    
    # === VERGELIJKING ===
    print(f"\n📊 VERGELIJKING:")
    print(f"- Projectlines totaal: {projectlines_total:,.2f} uren")
    print(f"- Urenregistratie totaal (alle): {urenregistratie_total_all:,.2f} uren")
    print(f"- Urenregistratie totaal (gefilterd): {urenregistratie_total_gefiatteerd:,.2f} uren")
    
    # Verschillen
    verschil_all = urenregistratie_total_all - projectlines_total
    verschil_gefiatteerd = urenregistratie_total_gefiatteerd - projectlines_total
    
    print(f"\n🔍 VERSCHILLEN:")
    print(f"- Projectlines vs Urenregistratie (alle): {verschil_all:+,.2f} uren")
    print(f"- Projectlines vs Urenregistratie (gefilterd): {verschil_gefiatteerd:+,.2f} uren")
    
    # === SAMENVATTING ===
    print(f"\n📋 SAMENVATTING:")
    print(f"🏢 Korff Dakwerken Volendam B.V. (ID: {korff_id})")
    print(f"📊 Geplande uren (projectlines): {projectlines_total:,.2f}")
    print(f"📊 Werkelijk gewerkte uren (alle): {urenregistratie_total_all:,.2f}")
    print(f"📊 Werkelijk gewerkte uren (gefilterd): {urenregistratie_total_gefiatteerd:,.2f}")
    
    if verschil_gefiatteerd < 0:
        print(f"📝 Nog te werken: {abs(verschil_gefiatteerd):,.2f} uren")
    elif verschil_gefiatteerd > 0:
        print(f"⚠️ Overschrijding: {verschil_gefiatteerd:,.2f} uren")
    else:
        print(f"✅ Exact overeenkomst")

if __name__ == "__main__":
    calculate_korff_totals()

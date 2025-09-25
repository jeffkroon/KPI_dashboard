#!/usr/bin/env python3
"""
Script om te onderzoeken wat het grote verschil in uren kan verklaren
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_hours_difference():
    """Onderzoek wat het grote verschil in uren kan verklaren"""
    
    print("🔍 Onderzoek: Wat verklaart het grote verschil in uren?")
    print("=" * 60)
    
    # Load data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "hidefortimewriting"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "date_date", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie)} records")
    print(f"- Projects: {len(df_projects)} records")
    
    # === 1. TOTAAL VERGELIJKING ===
    print(f"\n📊 TOTAAL VERGELIJKING:")
    
    # Projectlines totaal
    df_projectlines_uren = df_projectlines[df_projectlines['unit_searchname'] == 'uur'].copy()
    df_projectlines_uren['amountwritten'] = pd.to_numeric(df_projectlines_uren['amountwritten'], errors='coerce')
    total_projectlines = df_projectlines_uren['amountwritten'].sum()
    
    # Urenregistratie totaal (alle statuses)
    df_urenregistratie['amount'] = pd.to_numeric(df_urenregistratie['amount'], errors='coerce')
    total_urenregistratie_all = df_urenregistratie['amount'].sum()
    
    # Urenregistratie totaal (alleen gefiatteerd)
    df_urenregistratie_gefiatteerd = df_urenregistratie[df_urenregistratie['status_searchname'] == 'Gefiatteerd']
    total_urenregistratie_gefiatteerd = df_urenregistratie_gefiatteerd['amount'].sum()
    
    print(f"- Projectlines totaal: {total_projectlines:,.2f} uren")
    print(f"- Urenregistratie totaal (alle): {total_urenregistratie_all:,.2f} uren")
    print(f"- Urenregistratie totaal (gefilterd): {total_urenregistratie_gefiatteerd:,.2f} uren")
    
    # Verschillen
    verschil_all = total_urenregistratie_all - total_projectlines
    verschil_gefiatteerd = total_urenregistratie_gefiatteerd - total_projectlines
    
    print(f"\n🔍 VERSCHILLEN:")
    print(f"- Projectlines vs Urenregistratie (alle): {verschil_all:+,.2f} uren")
    print(f"- Projectlines vs Urenregistratie (gefilterd): {verschil_gefiatteerd:+,.2f} uren")
    
    # === 2. STATUS BREAKDOWN ===
    print(f"\n📋 STATUS BREAKDOWN:")
    status_breakdown = df_urenregistratie['status_searchname'].value_counts()
    for status, count in status_breakdown.items():
        status_total = df_urenregistratie[df_urenregistratie['status_searchname'] == status]['amount'].sum()
        print(f"  - {status}: {count} records, {status_total:,.2f} uren")
    
    # === 3. PROJECT MATCHING ===
    print(f"\n🔗 PROJECT MATCHING:")
    
    # Projecten met projectlines
    projects_with_projectlines = df_projectlines_uren['offerprojectbase_id'].unique()
    print(f"- Projecten met projectlines: {len(projects_with_projectlines)}")
    
    # Projecten met urenregistratie
    projects_with_urenregistratie = df_urenregistratie['offerprojectbase_id'].unique()
    print(f"- Projecten met urenregistratie: {len(projects_with_urenregistratie)}")
    
    # Overlap
    overlap = set(projects_with_projectlines) & set(projects_with_urenregistratie)
    print(f"- Projecten met beide: {len(overlap)}")
    
    # Alleen projectlines
    only_projectlines = set(projects_with_projectlines) - set(projects_with_urenregistratie)
    print(f"- Alleen projectlines: {len(only_projectlines)}")
    
    # Alleen urenregistratie
    only_urenregistratie = set(projects_with_urenregistratie) - set(projects_with_projectlines)
    print(f"- Alleen urenregistratie: {len(only_urenregistratie)}")
    
    # === 4. BEDRIJF MATCHING ===
    print(f"\n🏢 BEDRIJF MATCHING:")
    
    # Bedrijven met projectlines
    bedrijven_projectlines = df_projectlines_uren['bedrijf_id'].unique()
    print(f"- Bedrijven met projectlines: {len(bedrijven_projectlines)}")
    
    # Bedrijven met urenregistratie (via projects)
    df_uren_with_company = df_urenregistratie.merge(df_projects, left_on="offerprojectbase_id", right_on="id", how="left")
    bedrijven_urenregistratie = df_uren_with_company['company_id'].dropna().unique()
    print(f"- Bedrijven met urenregistratie: {len(bedrijven_urenregistratie)}")
    
    # Overlap
    overlap_bedrijven = set(bedrijven_projectlines) & set(bedrijven_urenregistratie)
    print(f"- Bedrijven met beide: {len(overlap_bedrijven)}")
    
    # Alleen projectlines
    only_projectlines_bedrijven = set(bedrijven_projectlines) - set(bedrijven_urenregistratie)
    print(f"- Alleen projectlines: {len(only_projectlines_bedrijven)}")
    
    # Alleen urenregistratie
    only_urenregistratie_bedrijven = set(bedrijven_urenregistratie) - set(bedrijven_projectlines)
    print(f"- Alleen urenregistratie: {len(only_urenregistratie_bedrijven)}")
    
    # === 5. DATUM ANALYSE ===
    print(f"\n📅 DATUM ANALYSE:")
    
    if 'date_date' in df_urenregistratie.columns:
        df_urenregistratie['date_date'] = pd.to_datetime(df_urenregistratie['date_date'], errors='coerce')
        
        # Datum range
        min_date = df_urenregistratie['date_date'].min()
        max_date = df_urenregistratie['date_date'].max()
        print(f"- Datum range: {min_date} tot {max_date}")
        
        # Per jaar
        df_urenregistratie['year'] = df_urenregistratie['date_date'].dt.year
        yearly_breakdown = df_urenregistratie.groupby('year')['amount'].sum()
        print(f"- Per jaar:")
        for year, total in yearly_breakdown.items():
            if pd.notna(year):
                print(f"  - {year}: {total:,.2f} uren")
    
    # === 6. MOGELIJKE OORZAKEN ===
    print(f"\n🔍 MOGELIJKE OORZAKEN VAN HET VERSCHIL:")
    
    print(f"1. **Status filtering**: {total_urenregistratie_all - total_urenregistratie_gefiatteerd:,.2f} uren worden weggefilterd")
    print(f"2. **Project mismatch**: {len(only_projectlines)} projecten hebben alleen projectlines")
    print(f"3. **Bedrijf mismatch**: {len(only_projectlines_bedrijven)} bedrijven hebben alleen projectlines")
    print(f"4. **Datum filtering**: App filtert op geselecteerde periode")
    print(f"5. **Data type verschil**: Projectlines = geplande uren, Urenregistratie = werkelijk gewerkte uren")
    
    # === 7. AANBEVELINGEN ===
    print(f"\n💡 AANBEVELINGEN:")
    print(f"- Gebruik 'amountwritten' in plaats van 'amount' voor projectlines")
    print(f"- Overweeg om beide bronnen te combineren voor volledig beeld")
    print(f"- Filter projectlines op 'hidefortimewriting = False'")
    print(f"- Controleer of alle projecten correct zijn gekoppeld")

if __name__ == "__main__":
    investigate_hours_difference()

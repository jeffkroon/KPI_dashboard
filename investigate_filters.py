#!/usr/bin/env python3
"""
Script om te onderzoeken welke filters we kunnen toepassen op projectlines om op hetzelfde aantal uit te komen als urenregistratie
"""

import pandas as pd
from utils.data_loaders import load_data_df

def investigate_projectlines_filters():
    """Onderzoek welke filters we kunnen toepassen op projectlines"""
    
    print("🔍 Onderzoek: Filters op projectlines om op urenregistratie uit te komen")
    print("=" * 70)
    
    korff_id = 95837  # Korff Dakwerken Volendam B.V.
    target_uren = 272.05  # Doel: urenregistratie totaal
    
    # Load projectlines data met alle mogelijke kolommen
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amountwritten", "unit_searchname", "amount", "sellingprice", "hidefortimewriting"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_urenregistratie = load_data_df("urenregistratie", columns=["employee_id", "offerprojectbase_id", "amount", "status_searchname"])
    if not isinstance(df_urenregistratie, pd.DataFrame):
        df_urenregistratie = pd.concat(list(df_urenregistratie), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Urenregistratie: {len(df_urenregistratie)} records")
    print(f"- Projects: {len(df_projects)} records")
    
    # Filter voor Korff
    df_projectlines_korff = df_projectlines[df_projectlines['bedrijf_id'] == korff_id].copy()
    df_projectlines_korff['amountwritten'] = pd.to_numeric(df_projectlines_korff['amountwritten'], errors='coerce')
    df_projectlines_korff['amount'] = pd.to_numeric(df_projectlines_korff['amount'], errors='coerce')
    
    print(f"\n🔍 Projectlines voor Korff: {len(df_projectlines_korff)} records")
    
    # Check alle kolommen en hun waarden
    print(f"\n📋 Kolommen in projectlines:")
    for col in df_projectlines_korff.columns:
        unique_values = df_projectlines_korff[col].value_counts()
        print(f"\n{col}:")
        for value, count in unique_values.items():
            print(f"  - '{value}': {count} records")
    
    # Test verschillende filters
    print(f"\n🧪 TESTEN VAN VERSCHILLENDE FILTERS:")
    
    # 1. Basis filter: alleen uren
    df_uren = df_projectlines_korff[df_projectlines_korff['unit_searchname'] == 'uur'].copy()
    total_uren = df_uren['amountwritten'].sum()
    print(f"1. Alleen 'uur' unit: {total_uren:,.2f} uren ({len(df_uren)} records)")
    
    # 2. Test hidefortimewriting filter
    if 'hidefortimewriting' in df_uren.columns:
        df_uren_not_hidden = df_uren[df_uren['hidefortimewriting'] == False].copy()
        total_not_hidden = df_uren_not_hidden['amountwritten'].sum()
        print(f"2. hidefortimewriting = False: {total_not_hidden:,.2f} uren ({len(df_uren_not_hidden)} records)")
        
        df_uren_hidden = df_uren[df_uren['hidefortimewriting'] == True].copy()
        total_hidden = df_uren_hidden['amountwritten'].sum()
        print(f"   hidefortimewriting = True: {total_hidden:,.2f} uren ({len(df_uren_hidden)} records)")
    
    # 3. Test amount vs amountwritten
    if 'amount' in df_uren.columns:
        total_amount = df_uren['amount'].sum()
        print(f"3. amount kolom: {total_amount:,.2f} uren")
    
    # 4. Test per project
    print(f"\n🔍 PER PROJECT BREAKDOWN:")
    projects_korff = df_projects[df_projects['company_id'] == korff_id]
    
    for _, project in projects_korff.iterrows():
        project_id = project['id']
        project_name = project['name']
        archived = " (Gearchiveerd)" if project['archived'] else ""
        
        # Projectlines voor dit project
        project_projectlines = df_uren[df_uren['offerprojectbase_id'] == project_id]
        projectlines_total = project_projectlines['amountwritten'].sum()
        
        # Urenregistratie voor dit project
        project_urenregistratie = df_urenregistratie[df_urenregistratie['offerprojectbase_id'] == project_id]
        project_urenregistratie['amount'] = pd.to_numeric(project_urenregistratie['amount'], errors='coerce')
        urenregistratie_total = project_urenregistratie['amount'].sum()
        
        print(f"\n🏢 {project_name}{archived} (ID: {project_id}):")
        print(f"   - Projectlines: {projectlines_total:,.2f} uren ({len(project_projectlines)} records)")
        print(f"   - Urenregistratie: {urenregistratie_total:,.2f} uren ({len(project_urenregistratie)} records)")
        
        # Test verschillende filters per project
        if len(project_projectlines) > 0:
            if 'hidefortimewriting' in project_projectlines.columns:
                not_hidden = project_projectlines[project_projectlines['hidefortimewriting'] == False]['amountwritten'].sum()
                print(f"   - hidefortimewriting=False: {not_hidden:,.2f} uren")
            
            if 'amount' in project_projectlines.columns:
                amount_total = project_projectlines['amount'].sum()
                print(f"   - amount kolom: {amount_total:,.2f} uren")
    
    # 5. Test combinaties van filters
    print(f"\n🧪 COMBINATIES VAN FILTERS:")
    
    # Alleen uren + hidefortimewriting = False
    if 'hidefortimewriting' in df_uren.columns:
        df_filtered = df_uren[df_uren['hidefortimewriting'] == False].copy()
        total_filtered = df_filtered['amountwritten'].sum()
        print(f"4. uren + hidefortimewriting=False: {total_filtered:,.2f} uren ({len(df_filtered)} records)")
        
        # Check of dit dichter bij target komt
        diff_original = abs(total_uren - target_uren)
        diff_filtered = abs(total_filtered - target_uren)
        
        print(f"   - Verschil met target (272.05): {diff_original:,.2f} vs {diff_filtered:,.2f}")
        if diff_filtered < diff_original:
            print(f"   ✅ Dit filter brengt ons dichter bij het doel!")
        else:
            print(f"   ❌ Dit filter helpt niet")
    
    # 6. Test alleen actieve projecten (niet gearchiveerd)
    print(f"\n5. Alleen actieve projecten (niet gearchiveerd):")
    active_projects = projects_korff[projects_korff['archived'] == False]['id'].tolist()
    df_active = df_uren[df_uren['offerprojectbase_id'].isin(active_projects)].copy()
    total_active = df_active['amountwritten'].sum()
    print(f"   - Actieve projecten: {total_active:,.2f} uren ({len(df_active)} records)")
    
    # 7. Test alleen gearchiveerde projecten
    archived_projects = projects_korff[projects_korff['archived'] == True]['id'].tolist()
    df_archived = df_uren[df_uren['offerprojectbase_id'].isin(archived_projects)].copy()
    total_archived = df_archived['amountwritten'].sum()
    print(f"   - Gearchiveerde projecten: {total_archived:,.2f} uren ({len(df_archived)} records)")

if __name__ == "__main__":
    investigate_projectlines_filters()

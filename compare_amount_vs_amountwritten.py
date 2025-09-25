#!/usr/bin/env python3
"""
Script om amount vs amountwritten te vergelijken in projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df

def compare_amount_vs_amountwritten():
    """Vergelijk amount vs amountwritten in projectlines"""
    
    print("🔍 Vergelijking: amount vs amountwritten in projectlines")
    print("=" * 60)
    
    korff_id = 95837  # Korff Dakwerken Volendam B.V.
    
    # Load projectlines data met beide kolommen
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "unit_searchname", "hidefortimewriting"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projects = load_data_df("projects", columns=["id", "company_id", "name", "archived"])
    if not isinstance(df_projects, pd.DataFrame):
        df_projects = pd.concat(list(df_projects), ignore_index=True)
    
    # Filter voor Korff
    df_projectlines_korff = df_projectlines[df_projectlines['bedrijf_id'] == korff_id].copy()
    
    print(f"📊 Projectlines voor Korff: {len(df_projectlines_korff)} records")
    
    # Converteer beide naar numeriek
    df_projectlines_korff['amount'] = pd.to_numeric(df_projectlines_korff['amount'], errors='coerce')
    df_projectlines_korff['amountwritten'] = pd.to_numeric(df_projectlines_korff['amountwritten'], errors='coerce')
    
    # Filter op uren
    df_uren = df_projectlines_korff[df_projectlines_korff['unit_searchname'] == 'uur'].copy()
    
    print(f"📊 Records met uren: {len(df_uren)}")
    
    # Totaal vergelijking
    total_amount = df_uren['amount'].sum()
    total_amountwritten = df_uren['amountwritten'].sum()
    verschil = total_amount - total_amountwritten
    
    print(f"\n📊 TOTAAL VERGELIJKING:")
    print(f"- amount: {total_amount:,.2f} uren")
    print(f"- amountwritten: {total_amountwritten:,.2f} uren")
    print(f"- Verschil: {verschil:+,.2f} uren")
    
    # Per record vergelijking
    print(f"\n🔍 PER RECORD VERGELIJKING:")
    df_uren['verschil'] = df_uren['amount'] - df_uren['amountwritten']
    
    # Categoriseer records
    exact_match = df_uren[df_uren['verschil'] == 0]
    amount_hoger = df_uren[df_uren['verschil'] > 0]
    amountwritten_hoger = df_uren[df_uren['verschil'] < 0]
    
    print(f"✅ Exact overeenkomst: {len(exact_match)} records")
    print(f"📈 amount > amountwritten: {len(amount_hoger)} records")
    print(f"📉 amount < amountwritten: {len(amountwritten_hoger)} records")
    
    # Details per categorie
    if len(exact_match) > 0:
        print(f"\n✅ EXACT OVEREENKOMST ({len(exact_match)} records):")
        for _, row in exact_match.iterrows():
            print(f"  - Project {row['offerprojectbase_id']}: {row['amount']:,.2f} uren")
    
    if len(amount_hoger) > 0:
        print(f"\n📈 AMOUNT HOGER ({len(amount_hoger)} records):")
        for _, row in amount_hoger.iterrows():
            print(f"  - Project {row['offerprojectbase_id']}: {row['amount']:,.2f} vs {row['amountwritten']:,.2f} (+{row['verschil']:,.2f})")
    
    if len(amountwritten_hoger) > 0:
        print(f"\n📉 AMOUNTWRITTEN HOGER ({len(amountwritten_hoger)} records):")
        for _, row in amountwritten_hoger.iterrows():
            print(f"  - Project {row['offerprojectbase_id']}: {row['amount']:,.2f} vs {row['amountwritten']:,.2f} ({row['verschil']:,.2f})")
    
    # Per project vergelijking
    print(f"\n🔍 PER PROJECT VERGELIJKING:")
    projects_korff = df_projects[df_projects['company_id'] == korff_id]
    
    for _, project in projects_korff.iterrows():
        project_id = project['id']
        project_name = project['name']
        archived = " (Gearchiveerd)" if project['archived'] else ""
        
        # Projectlines voor dit project
        project_projectlines = df_uren[df_uren['offerprojectbase_id'] == project_id]
        
        if len(project_projectlines) > 0:
            project_amount = project_projectlines['amount'].sum()
            project_amountwritten = project_projectlines['amountwritten'].sum()
            project_verschil = project_amount - project_amountwritten
            
            print(f"\n🏢 {project_name}{archived} (ID: {project_id}):")
            print(f"   - amount: {project_amount:,.2f} uren ({len(project_projectlines)} records)")
            print(f"   - amountwritten: {project_amountwritten:,.2f} uren")
            print(f"   - Verschil: {project_verschil:+,.2f} uren")
            
            # Per record binnen project
            if len(project_projectlines) > 1:
                print(f"   📋 Per record:")
                for _, record in project_projectlines.iterrows():
                    print(f"     - Record: {record['amount']:,.2f} vs {record['amountwritten']:,.2f} ({record['amount'] - record['amountwritten']:+,.2f})")
    
    # Test met hidefortimewriting filter
    print(f"\n🧪 TEST MET HIDEFORTIMEWRITING FILTER:")
    
    # Alleen amount
    df_uren_not_hidden_amount = df_uren[df_uren['hidefortimewriting'] == False]['amount'].sum()
    df_uren_hidden_amount = df_uren[df_uren['hidefortimewriting'] == True]['amount'].sum()
    
    # Alleen amountwritten
    df_uren_not_hidden_amountwritten = df_uren[df_uren['hidefortimewriting'] == False]['amountwritten'].sum()
    df_uren_hidden_amountwritten = df_uren[df_uren['hidefortimewriting'] == True]['amountwritten'].sum()
    
    print(f"- amount (hidefortimewriting=False): {df_uren_not_hidden_amount:,.2f} uren")
    print(f"- amount (hidefortimewriting=True): {df_uren_hidden_amount:,.2f} uren")
    print(f"- amountwritten (hidefortimewriting=False): {df_uren_not_hidden_amountwritten:,.2f} uren")
    print(f"- amountwritten (hidefortimewriting=True): {df_uren_hidden_amountwritten:,.2f} uren")
    
    # Vergelijk met urenregistratie target
    target_uren = 272.05  # urenregistratie totaal
    
    print(f"\n🎯 VERGELIJKING MET URENREGISTRATIE TARGET ({target_uren:,.2f}):")
    print(f"- amount: {total_amount:,.2f} (verschil: {total_amount - target_uren:+,.2f})")
    print(f"- amountwritten: {total_amountwritten:,.2f} (verschil: {total_amountwritten - target_uren:+,.2f})")
    print(f"- amount (hidefortimewriting=False): {df_uren_not_hidden_amount:,.2f} (verschil: {df_uren_not_hidden_amount - target_uren:+,.2f})")
    print(f"- amountwritten (hidefortimewriting=False): {df_uren_not_hidden_amountwritten:,.2f} (verschil: {df_uren_not_hidden_amountwritten - target_uren:+,.2f})")

if __name__ == "__main__":
    compare_amount_vs_amountwritten()

#!/usr/bin/env python3
"""
Script om te testen of de app nu meer data toont met projectlines uren
"""

import pandas as pd
from utils.data_loaders import load_data_df
from datetime import datetime, date

def test_app_data():
    """Test of de app nu meer data toont met projectlines uren"""
    
    print("🧪 Test: App data met projectlines uren")
    print("=" * 50)
    
    # Simuleer de app logica
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    print(f"📅 Periode: {start_date.strftime('%Y-%m-%d')} tot {end_date.strftime('%Y-%m-%d')}")
    
    # Load data zoals in de app
    df_companies = load_data_df("companies", columns=["id", "companyname"])
    if not isinstance(df_companies, pd.DataFrame):
        df_companies = pd.concat(list(df_companies), ignore_index=True)
    
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "sellingprice", "unit_searchname", "createdon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
    if not isinstance(df_invoices, pd.DataFrame):
        df_invoices = pd.concat(list(df_invoices), ignore_index=True)
    
    print(f"📊 Data geladen:")
    print(f"- Companies: {len(df_companies)} records")
    print(f"- Projectlines: {len(df_projectlines)} records")
    print(f"- Invoices: {len(df_invoices)} records")
    
    # Simuleer bedrijf filtering (alle bedrijven)
    bedrijf_ids = df_companies["id"].unique().tolist()
    print(f"- Bedrijf IDs: {len(bedrijf_ids)} bedrijven")
    
    # Filter projectlines op bedrijf_ids
    df_projectlines = df_projectlines[df_projectlines["bedrijf_id"].isin(bedrijf_ids)]
    print(f"- Projectlines na bedrijf filtering: {len(df_projectlines)} records")
    
    # Filter projectlines op unit "uur" en bedrijf_ids
    df_projectlines_uren = df_projectlines[
        (df_projectlines["unit_searchname"] == "uur") &
        (df_projectlines["bedrijf_id"].isin(bedrijf_ids))
    ].copy()
    print(f"- Projectlines uren: {len(df_projectlines_uren)} records")
    
    # Filter projectlines op geselecteerde periode (als createdon_date beschikbaar is)
    if 'createdon_date' in df_projectlines_uren.columns:
        df_projectlines_uren['createdon_date'] = pd.to_datetime(df_projectlines_uren['createdon_date'], errors='coerce')
        # Alleen records met createdon_date filteren op periode
        df_projectlines_with_date = df_projectlines_uren[
            (df_projectlines_uren['createdon_date'].notna()) &
            (df_projectlines_uren['createdon_date'] >= start_date) &
            (df_projectlines_uren['createdon_date'] <= end_date)
        ]
        # Records zonder createdon_date toevoegen (geen datum filtering)
        df_projectlines_without_date = df_projectlines_uren[df_projectlines_uren['createdon_date'].isna()]
        # Combineer beide
        df_projectlines_filtered = pd.concat([df_projectlines_with_date, df_projectlines_without_date], ignore_index=True)
        
        print(f"- Records met createdon_date: {len(df_projectlines_with_date)}")
        print(f"- Records zonder createdon_date: {len(df_projectlines_without_date)}")
        print(f"- Totaal gefilterd: {len(df_projectlines_filtered)}")
    else:
        # Geen createdon_date kolom, gebruik alle projectlines
        df_projectlines_filtered = df_projectlines_uren
        print(f"- Geen createdon_date kolom, gebruik alle: {len(df_projectlines_filtered)}")
    
    # Bereken totaal uren per bedrijf
    df_projectlines_filtered["amountwritten"] = pd.to_numeric(df_projectlines_filtered["amountwritten"], errors="coerce")
    uren_per_bedrijf = df_projectlines_filtered.groupby("bedrijf_id")["amountwritten"].sum().reset_index()
    uren_per_bedrijf.columns = ["bedrijf_id", "totaal_uren"]
    
    print(f"\n📊 RESULTATEN:")
    print(f"- Bedrijven met uren: {len(uren_per_bedrijf)}")
    print(f"- Totaal uren: {uren_per_bedrijf['totaal_uren'].sum():,.2f}")
    print(f"- Gemiddeld uren per bedrijf: {uren_per_bedrijf['totaal_uren'].mean():,.2f}")
    
    # Top 10 bedrijven
    top10 = uren_per_bedrijf.nlargest(10, 'totaal_uren')
    print(f"\n🏆 TOP 10 BEDRIJVEN:")
    for idx, row in top10.iterrows():
        bedrijf_naam = df_companies[df_companies['id'] == row['bedrijf_id']]['companyname'].iloc[0] if len(df_companies[df_companies['id'] == row['bedrijf_id']]) > 0 else f"ID {row['bedrijf_id']}"
        print(f"  {bedrijf_naam}: {row['totaal_uren']:,.2f} uren")
    
    # Filter invoices op periode
    if 'reportdate_date' in df_invoices.columns:
        df_invoices['reportdate_date'] = pd.to_datetime(df_invoices['reportdate_date'], errors='coerce')
        df_invoices_filtered = df_invoices[
            (df_invoices['reportdate_date'] >= start_date) &
            (df_invoices['reportdate_date'] <= end_date) &
            (df_invoices['company_id'].isin(bedrijf_ids))
        ]
        print(f"\n💰 FACTUREN:")
        print(f"- Facturen in periode: {len(df_invoices_filtered)}")
        if len(df_invoices_filtered) > 0:
            total_amount = pd.to_numeric(df_invoices_filtered['totalpayed'], errors='coerce').sum()
            print(f"- Totaal factuurbedrag: €{total_amount:,.2f}")
    
    print(f"\n✅ CONCLUSIE:")
    print(f"- App gebruikt nu projectlines amountwritten voor uren berekening")
    print(f"- Dit zou veel meer data moeten tonen dan urenregistratie alleen")
    print(f"- Probleem: {len(df_projectlines_without_date) if 'createdon_date' in df_projectlines_uren.columns else 'onbekend'} records zonder createdon_date")

if __name__ == "__main__":
    test_app_data()

#!/usr/bin/env python3
"""
Test script om te onderzoeken waarom Korff Dakwerken 0 invoices heeft in de app
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.data_loaders import load_data_df

st.title("🔍 Test: Waarom heeft Korff Dakwerken 0 invoices?")

# Simuleer de exacte filtering uit app.py
max_date = datetime.today()
min_date_default = max_date - timedelta(days=30)

# Date input
date_range = st.date_input(
    "📅 Analyseperiode",
    (min_date_default, max_date),
    min_value=datetime(2020, 1, 1),
    max_value=max_date,
    help="Selecteer de periode die u wilt analyseren."
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date_default, max_date

start_date_dt = pd.to_datetime(start_date)
end_date_dt = pd.to_datetime(end_date)

st.write(f"**Geselecteerde periode:** {start_date} tot {end_date}")

# Simuleer de exacte filtering uit app.py
st.write("**Stap 1: Simuleer app.py filtering**")

# Laad bedrijven
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

st.write(f"- Totaal bedrijven in database: {len(df_companies)}")

# Zoek Korff Dakwerken
korff_companies = df_companies[df_companies['companyname'].str.contains('Korff', case=False, na=False)]
st.write(f"**Korff bedrijven gevonden:** {len(korff_companies)}")

if len(korff_companies) > 0:
    for idx, row in korff_companies.iterrows():
        st.write(f"- ID {row['id']}: {row['companyname']}")
        st.write(f"  - Tags: {row['tag_names']}")
    
    korff_id = korff_companies.iloc[0]['id']
    korff_name = korff_companies.iloc[0]['companyname']
    korff_tags = korff_companies.iloc[0]['tag_names']
    
    st.write(f"**Geselecteerd voor analyse:** {korff_name} (ID: {korff_id})")
    st.write(f"**Tags:** {korff_tags}")
    
    # Test verschillende filter opties
    st.write("**Stap 2: Test verschillende filter opties**")
    
    # Optie 1: Alle bedrijven (zoals in test_missing_invoices.py)
    df_companies_alle = df_companies[
        (df_companies["tag_names"].notna()) &
        (df_companies["tag_names"].str.strip() != "")
    ].copy()
    
    korff_in_alle = korff_id in df_companies_alle["id"].values
    st.write(f"- In 'Alle bedrijven' filter: {'✅ JA' if korff_in_alle else '❌ NEE'}")
    
    # Optie 2: Eigen bedrijven
    def bedrijf_heeft_tag(tag_string, filter_primary_tag):
        if not isinstance(tag_string, str):
            return False
        tags = [t.strip() for t in tag_string.split(",")]
        return filter_primary_tag in tags
    
    eigen_tag = "1 | Eigen webshop(s) / bedrijven"
    df_companies_eigen = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, eigen_tag))].copy()
    
    korff_in_eigen = korff_id in df_companies_eigen["id"].values
    st.write(f"- In 'Eigen bedrijven' filter: {'✅ JA' if korff_in_eigen else '❌ NEE'}")
    
    # Optie 3: Klanten
    klant_tag = "1 | Externe opdrachten / contracten"
    df_companies_klanten = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, klant_tag))].copy()
    
    korff_in_klanten = korff_id in df_companies_klanten["id"].values
    st.write(f"- In 'Klanten' filter: {'✅ JA' if korff_in_klanten else '❌ NEE'}")
    
    # Test invoices voor Korff
    st.write("**Stap 3: Test invoices voor Korff**")
    
    # Laad alle invoices
    df_invoices_all = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
    if not isinstance(df_invoices_all, pd.DataFrame):
        df_invoices_all = pd.concat(list(df_invoices_all), ignore_index=True)
    
    # Filter op Korff
    df_invoices_korff = df_invoices_all[df_invoices_all["company_id"] == korff_id].copy()
    st.write(f"- Totaal invoices voor Korff: {len(df_invoices_korff)}")
    
    if len(df_invoices_korff) > 0:
        # Check reportdate_date
        if 'reportdate_date' in df_invoices_korff.columns:
            df_invoices_korff['reportdate_date'] = pd.to_datetime(df_invoices_korff['reportdate_date'], errors='coerce')
            
            null_dates = df_invoices_korff['reportdate_date'].isna().sum()
            valid_dates = len(df_invoices_korff) - null_dates
            
            st.write(f"- Invoices met geldige reportdate_date: {valid_dates}")
            st.write(f"- Invoices zonder reportdate_date: {null_dates}")
            
            if valid_dates > 0:
                min_date_korff = df_invoices_korff['reportdate_date'].min()
                max_date_korff = df_invoices_korff['reportdate_date'].max()
                st.write(f"- Korff invoice datums: {min_date_korff} tot {max_date_korff}")
                
                # Filter op geselecteerde periode
                df_invoices_korff_filtered = df_invoices_korff[
                    (df_invoices_korff['reportdate_date'] >= start_date_dt) &
                    (df_invoices_korff['reportdate_date'] <= end_date_dt)
                ]
                
                st.write(f"- Invoices binnen geselecteerde periode: {len(df_invoices_korff_filtered)}")
                
                if len(df_invoices_korff_filtered) > 0:
                    total_amount = pd.to_numeric(df_invoices_korff_filtered['totalpayed'], errors='coerce').sum()
                    st.write(f"- Totaal bedrag: €{total_amount:,.2f}")
                    
                    # Toon voorbeelden
                    st.write("**Voorbeelden van Korff invoices in periode:**")
                    sample = df_invoices_korff_filtered[['number', 'reportdate_date', 'totalpayed', 'status_searchname']].head(10)
                    st.dataframe(sample, use_container_width=True)
                else:
                    st.warning("⚠️ Geen Korff invoices in de geselecteerde periode!")
                    
                    # Toon voorbeelden buiten periode
                    outside_period = df_invoices_korff[
                        (df_invoices_korff['reportdate_date'] < start_date_dt) | 
                        (df_invoices_korff['reportdate_date'] > end_date_dt)
                    ]
                    if len(outside_period) > 0:
                        st.write("**Korff invoices buiten periode:**")
                        sample_outside = outside_period[['number', 'reportdate_date', 'totalpayed']].head(10)
                        st.dataframe(sample_outside, use_container_width=True)
            else:
                st.warning("⚠️ Alle Korff invoices hebben geen reportdate_date!")
                st.write("**Korff invoices zonder datum:**")
                sample_no_date = df_invoices_korff[['number', 'totalpayed', 'status_searchname']].head(10)
                st.dataframe(sample_no_date, use_container_width=True)
    else:
        st.error("❌ Geen invoices gevonden voor Korff Dakwerken!")
    
    # Test projectlines voor Korff
    st.write("**Stap 4: Test projectlines voor Korff**")
    
    df_projectlines = load_data_df("projectlines_per_company", columns=["id", "bedrijf_id", "offerprojectbase_id", "amount", "amountwritten", "sellingprice", "unit_searchname", "createdon_date"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    df_projectlines_korff = df_projectlines[df_projectlines["bedrijf_id"] == korff_id].copy()
    st.write(f"- Totaal projectlines voor Korff: {len(df_projectlines_korff)}")
    
    if len(df_projectlines_korff) > 0:
        # Filter op uren
        df_projectlines_uren = df_projectlines_korff[df_projectlines_korff["unit_searchname"] == "uur"].copy()
        st.write(f"- Projectlines uren voor Korff: {len(df_projectlines_uren)}")
        
        if len(df_projectlines_uren) > 0:
            # Check createdon_date
            if 'createdon_date' in df_projectlines_uren.columns:
                df_projectlines_uren['createdon_date'] = pd.to_datetime(df_projectlines_uren['createdon_date'], errors='coerce')
                
                null_dates = df_projectlines_uren['createdon_date'].isna().sum()
                valid_dates = len(df_projectlines_uren) - null_dates
                
                st.write(f"- Projectlines met createdon_date: {valid_dates}")
                st.write(f"- Projectlines zonder createdon_date: {null_dates}")
                
                if valid_dates > 0:
                    min_date_proj = df_projectlines_uren['createdon_date'].min()
                    max_date_proj = df_projectlines_uren['createdon_date'].max()
                    st.write(f"- Projectlines datums: {min_date_proj} tot {max_date_proj}")
                    
                    # Filter op geselecteerde periode (zoals in app.py)
                    df_projectlines_with_date = df_projectlines_uren[
                        (df_projectlines_uren['createdon_date'].notna()) &
                        (df_projectlines_uren['createdon_date'] >= start_date_dt) &
                        (df_projectlines_uren['createdon_date'] <= end_date_dt)
                    ]
                    df_projectlines_without_date = df_projectlines_uren[df_projectlines_uren['createdon_date'].isna()]
                    df_projectlines_filtered = pd.concat([df_projectlines_with_date, df_projectlines_without_date], ignore_index=True)
                    
                    st.write(f"- Projectlines binnen periode: {len(df_projectlines_with_date)}")
                    st.write(f"- Projectlines zonder datum (toegevoegd): {len(df_projectlines_without_date)}")
                    st.write(f"- Totaal projectlines na filtering: {len(df_projectlines_filtered)}")
                    
                    if len(df_projectlines_filtered) > 0:
                        total_uren = pd.to_numeric(df_projectlines_filtered['amountwritten'], errors='coerce').sum()
                        st.write(f"- Totaal uren: {total_uren:,.2f}")
    
    # Conclusie
    st.write("**Stap 5: Conclusie**")
    
    if not korff_in_alle:
        st.error("❌ **PROBLEEM GEVONDEN:** Korff Dakwerken wordt uitgesloten door de tag filtering!")
        st.write("Dit verklaart waarom er 0 invoices zijn - het bedrijf wordt niet meegenomen in de filtering.")
    else:
        st.success("✅ Korff Dakwerken wordt wel meegenomen in de filtering.")
        if len(df_invoices_korff) == 0:
            st.error("❌ **PROBLEEM:** Geen invoices gevonden voor Korff in de database.")
        elif len(df_invoices_korff_filtered) == 0:
            st.warning("⚠️ **PROBLEEM:** Invoices gevonden maar niet in de geselecteerde periode.")

else:
    st.error("❌ Geen Korff bedrijven gevonden in de database!")

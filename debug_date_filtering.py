#!/usr/bin/env python3
"""
Debug script om te onderzoeken waarom er maar 1 Korff invoice is in plaats van 45
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.data_loaders import load_data_df

st.title("🔍 Debug: Date Filtering Issue")

# Test verschillende datum ranges
st.write("**Stap 1: Test verschillende datum ranges**")

# Laad data
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
if not isinstance(df_invoices, pd.DataFrame):
    df_invoices = pd.concat(list(df_invoices), ignore_index=True)

# Filter op bedrijven met tags (zoals in app.py "Alle bedrijven")
df_companies_filtered = df_companies[
    (df_companies["tag_names"].notna()) &
    (df_companies["tag_names"].str.strip() != "")
]
bedrijf_ids = df_companies_filtered["id"].unique().tolist()

# Filter invoices op bedrijf_ids
df_invoices_filtered = df_invoices[df_invoices["company_id"].isin(bedrijf_ids)].copy()

# Korff ID
korff_id = 95837

# Test 1: Geen datum filtering
st.write("**Test 1: Geen datum filtering**")
df_invoices_no_date_filter = df_invoices_filtered.copy()
korff_invoices_no_filter = df_invoices_no_date_filter[df_invoices_no_date_filter["company_id"] == korff_id]
st.write(f"- Korff invoices (geen datum filter): {len(korff_invoices_no_filter)}")

if len(korff_invoices_no_filter) > 0:
    if 'reportdate_date' in korff_invoices_no_filter.columns:
        korff_invoices_no_filter['reportdate_date'] = pd.to_datetime(korff_invoices_no_filter['reportdate_date'], errors='coerce')
        null_dates = korff_invoices_no_filter['reportdate_date'].isna().sum()
        valid_dates = len(korff_invoices_no_filter) - null_dates
        st.write(f"  - Met reportdate_date: {valid_dates}")
        st.write(f"  - Zonder reportdate_date: {null_dates}")
        
        if valid_dates > 0:
            min_date = korff_invoices_no_filter['reportdate_date'].min()
            max_date = korff_invoices_no_filter['reportdate_date'].max()
            st.write(f"  - Datum range: {min_date} tot {max_date}")

# Test 2: App.py default (laatste 30 dagen)
st.write("**Test 2: App.py default (laatste 30 dagen)**")
max_date = datetime.today()
min_date_default = max_date - timedelta(days=30)
start_date_dt = pd.to_datetime(min_date_default)
end_date_dt = pd.to_datetime(max_date)

st.write(f"- Datum range: {min_date_default} tot {max_date}")

if 'reportdate_date' in df_invoices_filtered.columns:
    df_invoices_filtered['reportdate_date'] = pd.to_datetime(df_invoices_filtered['reportdate_date'], errors='coerce')
    df_invoices_app_filter = df_invoices_filtered[
        (df_invoices_filtered['reportdate_date'] >= start_date_dt) &
        (df_invoices_filtered['reportdate_date'] <= end_date_dt)
    ]
    
    korff_invoices_app = df_invoices_app_filter[df_invoices_app_filter["company_id"] == korff_id]
    st.write(f"- Korff invoices (app filter): {len(korff_invoices_app)}")
    
    if len(korff_invoices_app) > 0:
        total_amount = pd.to_numeric(korff_invoices_app['totalpayed'], errors='coerce').sum()
        st.write(f"- Totaal bedrag: €{total_amount:,.2f}")
        
        # Toon voorbeelden
        st.write("**Voorbeelden:**")
        sample = korff_invoices_app[['number', 'reportdate_date', 'totalpayed']].head(5)
        st.dataframe(sample, use_container_width=True)

# Test 3: Brede datum range (zoals in test_missing_invoices.py)
st.write("**Test 3: Brede datum range (2020-2025)**")
start_date_wide = pd.to_datetime("2020-01-01")
end_date_wide = pd.to_datetime("2025-12-31")

st.write(f"- Datum range: {start_date_wide} tot {end_date_wide}")

if 'reportdate_date' in df_invoices_filtered.columns:
    df_invoices_wide_filter = df_invoices_filtered[
        (df_invoices_filtered['reportdate_date'] >= start_date_wide) &
        (df_invoices_filtered['reportdate_date'] <= end_date_wide)
    ]
    
    korff_invoices_wide = df_invoices_wide_filter[df_invoices_wide_filter["company_id"] == korff_id]
    st.write(f"- Korff invoices (brede filter): {len(korff_invoices_wide)}")
    
    if len(korff_invoices_wide) > 0:
        total_amount = pd.to_numeric(korff_invoices_wide['totalpayed'], errors='coerce').sum()
        st.write(f"- Totaal bedrag: €{total_amount:,.2f}")
        
        # Toon voorbeelden
        st.write("**Voorbeelden:**")
        sample = korff_invoices_wide[['number', 'reportdate_date', 'totalpayed']].head(10)
        st.dataframe(sample, use_container_width=True)

# Test 4: Vergelijk met test_missing_invoices.py periode
st.write("**Test 4: Test_missing_invoices.py periode (2020-08-01 tot 2025-08-31)**")
start_date_test = pd.to_datetime("2020-08-01")
end_date_test = pd.to_datetime("2025-08-31")

st.write(f"- Datum range: {start_date_test} tot {end_date_test}")

if 'reportdate_date' in df_invoices_filtered.columns:
    df_invoices_test_filter = df_invoices_filtered[
        (df_invoices_filtered['reportdate_date'] >= start_date_test) &
        (df_invoices_filtered['reportdate_date'] <= end_date_test)
    ]
    
    korff_invoices_test = df_invoices_test_filter[df_invoices_test_filter["company_id"] == korff_id]
    st.write(f"- Korff invoices (test filter): {len(korff_invoices_test)}")
    
    if len(korff_invoices_test) > 0:
        total_amount = pd.to_numeric(korff_invoices_test['totalpayed'], errors='coerce').sum()
        st.write(f"- Totaal bedrag: €{total_amount:,.2f}")

# Test 5: Toon alle Korff invoices met datums
st.write("**Test 5: Alle Korff invoices met datums**")
if len(korff_invoices_no_filter) > 0 and 'reportdate_date' in korff_invoices_no_filter.columns:
    korff_with_dates = korff_invoices_no_filter[korff_invoices_no_filter['reportdate_date'].notna()].copy()
    korff_with_dates = korff_with_dates.sort_values('reportdate_date')
    
    st.write(f"- Totaal Korff invoices met datums: {len(korff_with_dates)}")
    
    # Toon alle datums
    st.write("**Alle Korff invoice datums:**")
    date_summary = korff_with_dates[['number', 'reportdate_date', 'totalpayed']].copy()
    st.dataframe(date_summary, use_container_width=True)
    
    # Groepeer per jaar
    korff_with_dates['year'] = korff_with_dates['reportdate_date'].dt.year
    yearly_summary = korff_with_dates.groupby('year').agg({
        'id': 'count',
        'totalpayed': lambda x: pd.to_numeric(x, errors='coerce').sum()
    }).rename(columns={'id': 'count'})
    
    st.write("**Korff invoices per jaar:**")
    st.dataframe(yearly_summary, use_container_width=True)

# Conclusie
st.write("**Conclusie:**")
st.write("De app.py gebruikt standaard de laatste 30 dagen, terwijl de test een veel bredere periode gebruikt.")
st.write("Dit verklaart waarom er maar 1 Korff invoice zichtbaar is in de app in plaats van 45.")
st.write("**Oplossing:** Verander de datum range in de app naar een bredere periode om alle Korff invoices te zien.")

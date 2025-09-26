#!/usr/bin/env python3
"""
Test script om te onderzoeken waarom er invoices missen in de app.py
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.data_loaders import load_data_df

st.title("🔍 Test: Waarom missen er invoices?")

# Simuleer dezelfde filtering als in app.py
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

# Laad bedrijven (met tag filtering zoals in app.py)
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

# Simuleer de exacte filtering uit app.py
# In app.py wordt dit alleen gedaan als filter_optie == "Alle bedrijven"
# Voor de test nemen we aan dat we "Alle bedrijven" willen zien
filter_optie = "Alle bedrijven"  # Simuleer deze keuze

if filter_optie == "Alle bedrijven":
    # Neem alleen bedrijven mee met geldige tags (behalve lege tags)
    df_companies = df_companies[
        (df_companies["tag_names"].notna()) &
        (df_companies["tag_names"].str.strip() != "")
    ]

bedrijf_ids = df_companies["id"].unique().tolist()
st.write(f"**Bedrijven met tags:** {len(bedrijf_ids)}")

# Laad alle invoices voor deze bedrijven
st.write("**Stap 1: Laad alle invoices voor deze bedrijven**")
df_invoices_all = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
if not isinstance(df_invoices_all, pd.DataFrame):
    df_invoices_all = pd.concat(list(df_invoices_all), ignore_index=True)

st.write(f"- Totaal invoices in database: {len(df_invoices_all)}")

# Filter op bedrijf_ids
df_invoices_all = df_invoices_all[df_invoices_all["company_id"].isin(bedrijf_ids)]
st.write(f"- Invoices voor geselecteerde bedrijven: {len(df_invoices_all)}")

if len(df_invoices_all) > 0:
    # Check reportdate_date kolom
    st.write("**Stap 2: Analyseer reportdate_date kolom**")
    
    if 'reportdate_date' in df_invoices_all.columns:
        # Convert to datetime
        df_invoices_all['reportdate_date'] = pd.to_datetime(df_invoices_all['reportdate_date'], errors='coerce')
        
        # Count null dates
        null_dates = df_invoices_all['reportdate_date'].isna().sum()
        valid_dates = len(df_invoices_all) - null_dates
        
        st.write(f"- Invoices met geldige reportdate_date: {valid_dates}")
        st.write(f"- Invoices zonder reportdate_date: {null_dates}")
        
        if null_dates > 0:
            st.write("**Invoices zonder datum:**")
            invoices_no_date = df_invoices_all[df_invoices_all['reportdate_date'].isna()][['number', 'company_id', 'totalpayed', 'status_searchname']]
            st.dataframe(invoices_no_date.head(10), use_container_width=True)
        
        # Check date range
        if valid_dates > 0:
            st.write("**Stap 3: Analyseer datumbereik**")
            
            min_date_all = df_invoices_all['reportdate_date'].min()
            max_date_all = df_invoices_all['reportdate_date'].max()
            
            st.write(f"- Alle invoice datums: {min_date_all} tot {max_date_all}")
            st.write(f"- Geselecteerde periode: {start_date} tot {end_date}")
            
            # Check hoeveel invoices buiten de periode vallen
            outside_period = df_invoices_all[
                (df_invoices_all['reportdate_date'] < start_date_dt) | 
                (df_invoices_all['reportdate_date'] > end_date_dt)
            ]
            
            inside_period = df_invoices_all[
                (df_invoices_all['reportdate_date'] >= start_date_dt) & 
                (df_invoices_all['reportdate_date'] <= end_date_dt)
            ]
            
            st.write(f"- Invoices binnen geselecteerde periode: {len(inside_period)}")
            st.write(f"- Invoices buiten geselecteerde periode: {len(outside_period)}")
            
            if len(outside_period) > 0:
                st.write("**Voorbeelden van invoices buiten periode:**")
                outside_sample = outside_period[['number', 'company_id', 'reportdate_date', 'totalpayed']].head(10)
                st.dataframe(outside_sample, use_container_width=True)
            
            if len(inside_period) > 0:
                st.write("**Voorbeelden van invoices binnen periode:**")
                inside_sample = inside_period[['number', 'company_id', 'reportdate_date', 'totalpayed']].head(10)
                st.dataframe(inside_sample, use_container_width=True)
                
                # Bereken totaal bedrag
                total_amount = pd.to_numeric(inside_period['totalpayed'], errors='coerce').sum()
                st.write(f"**Totaal bedrag binnen periode: €{total_amount:,.2f}**")
        
        # Simuleer de filtering zoals in app.py
        st.write("**Stap 4: Simuleer app.py filtering**")
        
        # Dit is de exacte filtering uit app.py
        df_invoices_filtered = df_invoices_all[
            (df_invoices_all['reportdate_date'] >= start_date_dt) &
            (df_invoices_all['reportdate_date'] <= end_date_dt)
        ]
        
        st.write(f"- Invoices na app.py filtering: {len(df_invoices_filtered)}")
        
        if len(df_invoices_filtered) > 0:
            total_filtered = pd.to_numeric(df_invoices_filtered['totalpayed'], errors='coerce').sum()
            st.write(f"- Totaal bedrag na filtering: €{total_filtered:,.2f}")
            
            # Toon per bedrijf
            st.write("**Invoices per bedrijf (na filtering):**")
            invoices_per_company = df_invoices_filtered.groupby("company_id")["totalpayed"].sum().reset_index()
            invoices_per_company = invoices_per_company.merge(df_companies[["id", "companyname"]], left_on="company_id", right_on="id", how="left")
            invoices_per_company = invoices_per_company[["companyname", "totalpayed"]].sort_values("totalpayed", ascending=False)
            st.dataframe(invoices_per_company, use_container_width=True)
    
    else:
        st.error("❌ Geen 'reportdate_date' kolom gevonden in invoices!")
        
else:
    st.warning("⚠️ Geen invoices gevonden voor de geselecteerde bedrijven!")

# Extra debug: Check andere datum kolommen
st.write("**Stap 5: Check andere datum kolommen**")
if len(df_invoices_all) > 0:
    st.write("**Beschikbare kolommen:**")
    st.write(list(df_invoices_all.columns))
    
    # Check date_date kolom
    if 'date_date' in df_invoices_all.columns:
        df_invoices_all['date_date'] = pd.to_datetime(df_invoices_all['date_date'], errors='coerce')
        null_date_date = df_invoices_all['date_date'].isna().sum()
        st.write(f"- Invoices zonder date_date: {null_date_date}")
        
        if null_date_date < len(df_invoices_all):
            min_date_date = df_invoices_all['date_date'].min()
            max_date_date = df_invoices_all['date_date'].max()
            st.write(f"- date_date bereik: {min_date_date} tot {max_date_date}")

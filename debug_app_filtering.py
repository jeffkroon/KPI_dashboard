#!/usr/bin/env python3
"""
Debug script om te zien welke filtering er precies wordt toegepast in de app
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.data_loaders import load_data_df

st.title("🔍 Debug: App Filtering Logic")

# Simuleer de exacte app.py logica
st.write("**Stap 1: Simuleer app.py filtering logica**")

# Laad bedrijven
df_companies = load_data_df("companies", columns=["id", "companyname", "tag_names"])
if not isinstance(df_companies, pd.DataFrame):
    df_companies = pd.concat(list(df_companies), ignore_index=True)

st.write(f"- Totaal bedrijven in database: {len(df_companies)}")

# Simuleer de radio button keuze
st.write("**Stap 2: Test verschillende filter opties (zoals in app.py)**")

# Helperfunctie voor exacte tag match (alleen primaire tag)
def bedrijf_heeft_tag(tag_string, filter_primary_tag):
    if not isinstance(tag_string, str):
        return False
    tags = [t.strip() for t in tag_string.split(",")]
    return filter_primary_tag in tags

# Test alle drie de opties
filter_opties = ["Alle bedrijven", "Eigen bedrijven", "Klanten"]

for filter_optie in filter_opties:
    st.write(f"**{filter_optie}:**")
    
    # Simuleer de exacte logica uit app.py
    filter_primary_tag = None
    if filter_optie == "Eigen bedrijven":
        filter_primary_tag = "1 | Eigen webshop(s) / bedrijven"
    elif filter_optie == "Klanten":
        filter_primary_tag = "1 | Externe opdrachten / contracten"
    
    # Pas filtering toe
    if filter_primary_tag:
        df_companies_filtered = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, filter_primary_tag))].copy()
    elif filter_optie == "Alle bedrijven":
        # Neem alleen bedrijven mee met geldige tags (behalve lege tags)
        df_companies_filtered = df_companies[
            (df_companies["tag_names"].notna()) &
            (df_companies["tag_names"].str.strip() != "")
        ].copy()
    else:
        df_companies_filtered = df_companies.copy()
    
    bedrijf_ids = df_companies_filtered["id"].unique().tolist()
    st.write(f"  - Bedrijven na filtering: {len(bedrijf_ids)}")
    
    # Check of Korff erin zit
    korff_id = 95837
    korff_in_filter = korff_id in bedrijf_ids
    st.write(f"  - Korff Dakwerken (ID {korff_id}) in filter: {'✅ JA' if korff_in_filter else '❌ NEE'}")
    
    if korff_in_filter:
        korff_company = df_companies_filtered[df_companies_filtered["id"] == korff_id].iloc[0]
        st.write(f"  - Korff tags: {korff_company['tag_names']}")
    
    st.write("")

# Test invoices voor elke filter optie
st.write("**Stap 3: Test invoices voor elke filter optie**")

# Laad invoices
df_invoices = load_data_df("invoices", columns=["id", "company_id", "fase", "totalpayed", "status_searchname", "number", "date_date", "reportdate_date", "subject"])
if not isinstance(df_invoices, pd.DataFrame):
    df_invoices = pd.concat(list(df_invoices), ignore_index=True)

# Simuleer datum filtering (laatste 30 dagen)
max_date = datetime.today()
min_date_default = max_date - timedelta(days=30)
start_date_dt = pd.to_datetime(min_date_default)
end_date_dt = pd.to_datetime(max_date)

st.write(f"**Datum filtering:** {min_date_default} tot {max_date}")

for filter_optie in filter_opties:
    st.write(f"**{filter_optie} - Invoices:**")
    
    # Herhaal filtering logica
    filter_primary_tag = None
    if filter_optie == "Eigen bedrijven":
        filter_primary_tag = "1 | Eigen webshop(s) / bedrijven"
    elif filter_optie == "Klanten":
        filter_primary_tag = "1 | Externe opdrachten / contracten"
    
    if filter_primary_tag:
        df_companies_filtered = df_companies[df_companies["tag_names"].apply(lambda x: bedrijf_heeft_tag(x, filter_primary_tag))].copy()
    elif filter_optie == "Alle bedrijven":
        df_companies_filtered = df_companies[
            (df_companies["tag_names"].notna()) &
            (df_companies["tag_names"].str.strip() != "")
        ].copy()
    else:
        df_companies_filtered = df_companies.copy()
    
    bedrijf_ids = df_companies_filtered["id"].unique().tolist()
    
    # Filter invoices op bedrijf_ids
    df_invoices_filtered = df_invoices[df_invoices["company_id"].isin(bedrijf_ids)].copy()
    
    # Filter invoices op geselecteerde periode
    if 'reportdate_date' in df_invoices_filtered.columns:
        df_invoices_filtered['reportdate_date'] = pd.to_datetime(df_invoices_filtered['reportdate_date'], errors='coerce')
        df_invoices_filtered = df_invoices_filtered[
            (df_invoices_filtered['reportdate_date'] >= start_date_dt) &
            (df_invoices_filtered['reportdate_date'] <= end_date_dt)
        ]
    
    st.write(f"  - Invoices na filtering: {len(df_invoices_filtered)}")
    
    # Check Korff invoices
    korff_invoices = df_invoices_filtered[df_invoices_filtered["company_id"] == 95837]
    st.write(f"  - Korff invoices: {len(korff_invoices)}")
    
    if len(korff_invoices) > 0:
        total_amount = pd.to_numeric(korff_invoices['totalpayed'], errors='coerce').sum()
        st.write(f"  - Korff totaal bedrag: €{total_amount:,.2f}")
    
    st.write("")

# Conclusie
st.write("**Stap 4: Conclusie**")
st.write("Deze test toont welke filter optie in de app actief moet zijn om Korff invoices te zien.")
st.write("Als je in de app 'Alle bedrijven' selecteert, zouden de Korff invoices zichtbaar moeten zijn.")
st.write("Als je 'Eigen bedrijven' selecteert, worden Korff invoices uitgesloten.")
st.write("Als je 'Klanten' selecteert, zouden Korff invoices zichtbaar moeten zijn.")

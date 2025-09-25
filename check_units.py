#!/usr/bin/env python3
"""
Script om te zien welke unit_searchname waarden er zijn in projectlines
"""

import pandas as pd
from utils.data_loaders import load_data_df

def check_unit_searchname():
    """Check welke unit_searchname waarden er zijn"""
    
    print("🔍 Unit searchname waarden in projectlines_per_company")
    print("=" * 60)
    
    # Load projectlines data
    df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "amountwritten", "unit_searchname"])
    if not isinstance(df_projectlines, pd.DataFrame):
        df_projectlines = pd.concat(list(df_projectlines), ignore_index=True)
    
    print(f"📊 Totaal projectlines records: {len(df_projectlines)}")
    
    # Check unique unit_searchname values
    unique_units = df_projectlines['unit_searchname'].value_counts()
    
    print(f"\n📋 Alle unit_searchname waarden:")
    for unit, count in unique_units.items():
        print(f"  - '{unit}': {count} records")
    
    # Check specifically for 'uur' variations
    print(f"\n🔍 Specifieke 'uur' checks:")
    uur_variations = ['uur', 'Uur', 'UUR', 'u', 'U']
    
    for variation in uur_variations:
        count = len(df_projectlines[df_projectlines['unit_searchname'] == variation])
        print(f"  - Exact '{variation}': {count} records")
    
    # Check case-insensitive
    count_lower = len(df_projectlines[df_projectlines['unit_searchname'].str.lower() == 'uur'])
    print(f"  - Case-insensitive 'uur': {count_lower} records")
    
    # Show some examples
    print(f"\n📝 Voorbeelden van unit_searchname waarden:")
    sample_units = df_projectlines['unit_searchname'].dropna().unique()[:10]
    for unit in sample_units:
        print(f"  - '{unit}'")

if __name__ == "__main__":
    check_unit_searchname()

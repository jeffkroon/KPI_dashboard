#!/usr/bin/env python3
"""
Testfile om projectlines op te halen via Gripp API offerprojectline.get
"""

import sys
import pandas as pd
import os
import time as pytime
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# === Configuratieparameters ===
load_dotenv()

BASE_URL = "https://api.gripp.com/public/api3.php"
GRIPP_API_KEY = os.getenv("GRIPP_API_KEY")
if not GRIPP_API_KEY:
    raise ValueError("GRIPP_API_KEY is not set in the environment.")
HEADERS = {"Authorization": f"Bearer {GRIPP_API_KEY}"}

def post_with_rate_limit_handling(*args, **kwargs):
    """Doet een requests.post, checkt op rate limit headers en status 429, en pauzeert indien nodig tot tokens zijn hersteld."""
    while True:
        response = requests.post(*args, **kwargs)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"⏳ Rate limit bereikt. Wacht {retry_after} seconden...")
            pytime.sleep(retry_after)
            continue
        return response

def fetch_offerprojectlines():
    """Haal projectlines op via offerprojectline.get API"""
    
    print("🔍 Projectlines ophalen via offerprojectline.get API...")
    
    all_rows = []
    start = 0
    max_results = 100
    watchdog = 50  # Maximaal 50 requests (5000 records)
    
    while watchdog > 0:
        print(f"📡 API call {start//max_results + 1}: records {start} tot {start + max_results - 1}")
        
        # API payload volgens specificatie
        payload = [{
            "method": "offerprojectline.get",
            "params": [
                [
                    {
                        "field": "offerprojectline.id",
                        "operator": "greaterequals",
                        "value": 1
                    }
                ],
                {
                    "paging": {
                        "firstresult": start,
                        "maxresults": max_results
                    },
                    "orderings": [
                        {
                            "field": "offerprojectline.id",
                            "direction": "asc"
                        }
                    ]
                }
            ],
            "id": 1
        }]
        
        # API call
        pytime.sleep(0.1)  # Rate limiting
        response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
        
        # Check response
        if response.status_code != 200:
            print(f"❌ API call failed: {response.status_code}")
            print(f"Response: {response.text}")
            break
        
        # Parse response
        try:
            data = response.json()
            print(f"🔍 Response data type: {type(data)}")
            print(f"🔍 Response data: {data}")
            
            if not data or len(data) == 0:
                print("❌ Geen data in response")
                break
            
            # Check response structure
            if isinstance(data, list) and len(data) > 0:
                first_item = data[0]
                print(f"🔍 First item type: {type(first_item)}")
                print(f"🔍 First item keys: {first_item.keys() if isinstance(first_item, dict) else 'Not a dict'}")
                
                if isinstance(first_item, dict):
                    result = first_item.get("result", [])
                    print(f"🔍 Result type: {type(result)}")
                    print(f"🔍 Result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
                    
                    if isinstance(result, dict):
                        # De result bevat metadata en rows
                        rows = result.get("rows", [])
                        count = result.get("count", 0)
                        print(f"🔍 Rows type: {type(rows)}")
                        print(f"🔍 Rows length: {len(rows)}")
                        print(f"🔍 Total count: {count}")
                        
                        if not rows:
                            print("✅ Geen meer data beschikbaar")
                            break
                        
                        print(f"📊 {len(rows)} records ontvangen")
                        all_rows.extend(rows)
                    else:
                        print(f"❌ Unexpected result structure: {result}")
                        break
                else:
                    print(f"❌ Unexpected response structure: {first_item}")
                    break
            else:
                print(f"❌ Unexpected response format: {data}")
                break
            
            # Check of we alle data hebben
            if len(all_rows) > 0 and len(all_rows[-len(result):]) < max_results:
                print("✅ Alle data opgehaald")
                break
            
            start += max_results
            watchdog -= 1
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            print(f"Response: {response.text}")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"📊 Totaal {len(all_rows)} projectlines opgehaald")
    
    # Converteer naar DataFrame
    if all_rows:
        print(f"🔍 All rows type: {type(all_rows)}")
        print(f"🔍 All rows length: {len(all_rows)}")
        print(f"🔍 First row: {all_rows[0] if all_rows else 'Empty'}")
        
        df = pd.DataFrame(all_rows)
        print(f"📋 DataFrame gemaakt: {len(df)} records, {len(df.columns)} kolommen")
        print(f"📋 Kolommen: {list(df.columns)}")
        
        # Toon eerste paar records
        print(f"\n📋 Eerste 3 records:")
        for idx, row in df.head(3).iterrows():
            print(f"  Record {idx + 1}:")
            for col in df.columns:
                print(f"    {col}: {row[col]}")
        
        # Toon statistieken
        print(f"\n📊 Statistieken:")
        print(f"- Totaal records: {len(df)}")
        print(f"- Unieke projectlines: {df['id'].nunique() if 'id' in df.columns else 'Onbekend'}")
        
        # Check voor amount/amountwritten kolommen
        amount_cols = [col for col in df.columns if isinstance(col, str) and 'amount' in col.lower()]
        if amount_cols:
            print(f"- Amount kolommen: {amount_cols}")
            for col in amount_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    total = df[col].sum()
                    print(f"  - {col}: {total:,.2f}")
        
        # Check voor unit kolommen
        unit_cols = [col for col in df.columns if isinstance(col, str) and 'unit' in col.lower()]
        if unit_cols:
            print(f"- Unit kolommen: {unit_cols}")
            for col in unit_cols:
                if col in df.columns:
                    # Extract unit names from nested dict structure
                    unit_names = []
                    for idx, row in df.iterrows():
                        if isinstance(row[col], dict) and 'searchname' in row[col]:
                            unit_names.append(row[col]['searchname'])
                        else:
                            unit_names.append('Unknown')
                    
                    unique_units = pd.Series(unit_names).value_counts()
                    print(f"  - {col}: {dict(unique_units)}")
        
        # Check voor datum kolommen
        date_cols = [col for col in df.columns if isinstance(col, str) and 'date' in col.lower()]
        if date_cols:
            print(f"- Datum kolommen: {date_cols}")
            for col in date_cols:
                if col in df.columns:
                    # Extract dates from nested dict structure
                    dates = []
                    for idx, row in df.iterrows():
                        if isinstance(row[col], dict) and 'date' in row[col]:
                            dates.append(row[col]['date'])
                        elif row[col] is not None:
                            dates.append(row[col])
                        else:
                            dates.append(None)
                    
                    df_temp = pd.DataFrame({'date': dates})
                    df_temp['date'] = pd.to_datetime(df_temp['date'], errors='coerce')
                    
                    valid_dates = df_temp['date'].dropna()
                    missing_dates = df_temp['date'].isna().sum()
                    
                    print(f"  - {col}:")
                    print(f"    - Records met datum: {len(valid_dates)}")
                    print(f"    - Records zonder datum: {missing_dates}")
                    if len(valid_dates) > 0:
                        print(f"    - Min datum: {valid_dates.min()}")
                        print(f"    - Max datum: {valid_dates.max()}")
        
        # Specifieke analyse van createdon datum
        if 'createdon' in df.columns:
            print(f"\n📅 **CREATEDON DATUM ANALYSE:**")
            createdon_dates = []
            for idx, row in df.iterrows():
                if isinstance(row['createdon'], dict) and 'date' in row['createdon']:
                    createdon_dates.append(row['createdon']['date'])
                else:
                    createdon_dates.append(None)
            
            df_createdon = pd.DataFrame({'createdon_date': createdon_dates})
            df_createdon['createdon_date'] = pd.to_datetime(df_createdon['createdon_date'], errors='coerce')
            
            valid_createdon = df_createdon['createdon_date'].dropna()
            missing_createdon = df_createdon['createdon_date'].isna().sum()
            
            print(f"- Totaal records: {len(df)}")
            print(f"- Records met createdon datum: {len(valid_createdon)}")
            print(f"- Records zonder createdon datum: {missing_createdon}")
            print(f"- Coverage: {len(valid_createdon)/len(df)*100:.1f}%")
            
            if len(valid_createdon) > 0:
                print(f"- Min createdon: {valid_createdon.min()}")
                print(f"- Max createdon: {valid_createdon.max()}")
                
                # Toon datum distributie per jaar
                valid_createdon_df = pd.DataFrame({'date': valid_createdon})
                valid_createdon_df['year'] = valid_createdon_df['date'].dt.year
                year_counts = valid_createdon_df['year'].value_counts().sort_index()
                print(f"- Datum distributie per jaar:")
                for year, count in year_counts.items():
                    print(f"  - {year}: {count} records")
        
        return df
    else:
        print("❌ Geen data opgehaald")
        return pd.DataFrame()

def test_offerprojectlines():
    """Test de offerprojectline.get API"""
    
    print("🧪 Test: offerprojectline.get API")
    print("=" * 50)
    
    try:
        df = fetch_offerprojectlines()
        
        if not df.empty:
            print(f"\n✅ Test succesvol!")
            print(f"📊 {len(df)} projectlines opgehaald")
            
            # Sla op als CSV voor inspectie
            output_file = "test_offerprojectlines.csv"
            df.to_csv(output_file, index=False)
            print(f"💾 Data opgeslagen als {output_file}")
            
            # Toon sample data
            print(f"\n📋 Sample data:")
            print(df.head().to_string())
            
        else:
            print("❌ Test gefaald: geen data opgehaald")
            
    except Exception as e:
        print(f"❌ Test gefaald: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_offerprojectlines()

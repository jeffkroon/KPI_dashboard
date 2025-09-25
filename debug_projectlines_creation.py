#!/usr/bin/env python3
"""
Debug script om te onderzoeken waarom createdon_date verloren gaat bij het maken van projectlines_per_company
"""

import pandas as pd
import sys
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

def fetch_projectlines_raw():
    """Haal projectlines op via Gripp API"""
    print("🔍 Projectlines ophalen via Gripp API...")
    
    all_rows = []
    first_result = 0
    max_results = 10  # Klein aantal voor debugging
    
    while True:
        payload = [
            {
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
                            "firstresult": first_result,
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
            }
        ]
        
        response = post_with_rate_limit_handling(BASE_URL, headers=HEADERS, json=payload)
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            break
        
        # Parse response
        try:
            data = response.json()
            if not data or len(data) == 0:
                print("❌ Geen data in response")
                break
            
            # Check response structure
            if isinstance(data, list) and len(data) > 0:
                first_item = data[0]
                if isinstance(first_item, dict):
                    result = first_item.get("result", {})
                    if isinstance(result, dict):
                        rows = result.get("rows", [])
                        if not rows:
                            print("✅ Geen meer data beschikbaar")
                            break
                        
                        print(f"📊 {len(rows)} records ontvangen")
                        all_rows.extend(rows)
                        
                        # Check of we alle data hebben
                        if len(rows) < max_results:
                            print("✅ Alle data opgehaald")
                            break
                        
                        first_result += max_results
                    else:
                        print("❌ Unexpected result structure")
                        break
                else:
                    print("❌ Unexpected response structure")
                    break
            else:
                print("❌ Unexpected data structure")
                break
                
        except Exception as e:
            print(f"❌ Error parsing response: {e}")
            break
    
    return all_rows

def analyze_raw_projectlines():
    """Analyseer de ruwe projectlines data"""
    print("🔍 RAW PROJECTLINES ANALYSE")
    print("=" * 50)
    
    # Haal ruwe data op
    raw_data = fetch_projectlines_raw()
    
    if not raw_data:
        print("❌ Geen data ontvangen")
        return
    
    print(f"📊 Totaal records: {len(raw_data)}")
    
    # Analyseer eerste record
    if raw_data:
        first_record = raw_data[0]
        print(f"\n🔍 Eerste record keys: {list(first_record.keys())}")
        
        # Check createdon kolom
        if 'createdon' in first_record:
            createdon_value = first_record['createdon']
            print(f"🔍 createdon type: {type(createdon_value)}")
            print(f"🔍 createdon value: {createdon_value}")
            
            if isinstance(createdon_value, dict):
                print(f"🔍 createdon dict keys: {list(createdon_value.keys())}")
                if 'date' in createdon_value:
                    print(f"🔍 createdon.date: {createdon_value['date']}")
        else:
            print("❌ Geen 'createdon' kolom gevonden!")
        
        # Check andere datum kolommen
        date_cols = [col for col in first_record.keys() if 'date' in col.lower()]
        print(f"🔍 Datum kolommen: {date_cols}")
        
        for col in date_cols:
            value = first_record[col]
            print(f"🔍 {col}: {type(value)} = {value}")
            if isinstance(value, dict):
                print(f"   Dict keys: {list(value.keys())}")
    
    # Converteer naar DataFrame voor verdere analyse
    df = pd.DataFrame(raw_data)
    print(f"\n📋 DataFrame gemaakt: {len(df)} records, {len(df.columns)} kolommen")
    
    # Check createdon kolom in DataFrame
    if 'createdon' in df.columns:
        print(f"\n🔍 createdon kolom analyse:")
        print(f"- Type: {df['createdon'].dtype}")
        print(f"- Non-null count: {df['createdon'].notna().sum()}")
        print(f"- Null count: {df['createdon'].isna().sum()}")
        
        # Check of het dict objects zijn
        createdon_types = df['createdon'].apply(type).value_counts()
        print(f"- Value types: {dict(createdon_types)}")
        
        # Probeer dates te extraheren
        dates = []
        for idx, row in df.iterrows():
            if isinstance(row['createdon'], dict) and 'date' in row['createdon']:
                dates.append(row['createdon']['date'])
            elif row['createdon'] is not None:
                dates.append(row['createdon'])
            else:
                dates.append(None)
        
        df_temp = pd.DataFrame({'date': dates})
        df_temp['date'] = pd.to_datetime(df_temp['date'], errors='coerce')
        
        print(f"- Dates extracted: {df_temp['date'].notna().sum()}")
        print(f"- Min date: {df_temp['date'].min()}")
        print(f"- Max date: {df_temp['date'].max()}")
    
    return df

if __name__ == "__main__":
    analyze_raw_projectlines()

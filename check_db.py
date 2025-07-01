import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('inhoud.env')

# Connect to database (same as in gripp_api.py)
POSTGRES_URL = os.getenv('POSTGRES_URL')
engine = create_engine(POSTGRES_URL)

print("=== CHECKING DATABASE VALUES ===")

with engine.connect() as conn:
    # Zoek een specifieke rij om te voorkomen dat de hele tabel wordt gescand
    result = conn.execute(text('''
        SELECT id, bedrijf_id, bedrijf_naam, offerprojectbase_id 
        FROM projectlines_per_company 
        WHERE id = 96
    '''))
    
    df = pd.DataFrame(result.fetchall(), columns=['id', 'bedrijf_id', 'bedrijf_naam', 'offerprojectbase_id'])
    print("Rij met id = 96:")
    print(df.to_string())
    
    # Probeer ook een andere rij
    result = conn.execute(text('''
        SELECT id, bedrijf_id, bedrijf_naam, offerprojectbase_id 
        FROM projectlines_per_company 
        WHERE id = 122
    '''))
    
    df2 = pd.DataFrame(result.fetchall(), columns=['id', 'bedrijf_id', 'bedrijf_naam', 'offerprojectbase_id'])
    print("\nRij met id = 122:")
    print(df2.to_string())
    
    # Check total counts
    result = conn.execute(text('''
        SELECT 
            COUNT(*) as total_rows,
            COUNT(bedrijf_id) as non_null_bedrijf_id,
            COUNT(bedrijf_naam) as non_null_bedrijf_naam
        FROM projectlines_per_company
    '''))
    
    counts = result.fetchone()
    print(f"\nTotal rows: {counts[0]}")
    print(f"Non-null bedrijf_id: {counts[1]}")
    print(f"Non-null bedrijf_naam: {counts[2]}")
    
    # Check a few sample rows
    result = conn.execute(text('''
        SELECT id, bedrijf_id, bedrijf_naam, offerprojectbase_id 
        FROM projectlines_per_company 
        LIMIT 5
    '''))
    
    sample_df = pd.DataFrame(result.fetchall(), columns=['id', 'bedrijf_id', 'bedrijf_naam', 'offerprojectbase_id'])
    print(f"\nSample rows:")
    print(sample_df.to_string())
    
    # Zoek in welk schema de tabel 'urenregistratie' staat
    result = conn.execute(text('''
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = 'urenregistratie';
    '''))
    df = pd.DataFrame(result.fetchall(), columns=['table_schema', 'table_name'])
    print("Schema info voor 'urenregistratie':")
    print(df.to_string(index=False))
    
    # Zoek het table_type van 'urenregistratie'
    result = conn.execute(text('''
        SELECT table_type FROM information_schema.tables WHERE table_name = 'urenregistratie';
    '''))
    df = pd.DataFrame(result.fetchall(), columns=['table_type'])
    print("Table type voor 'urenregistratie':")
    print(df.to_string(index=False))
    
    # Haal de kolomnamen van de tabel 'urenregistratie' op
    result = conn.execute(text('''
        SELECT column_name FROM information_schema.columns WHERE table_name = 'urenregistratie';
    '''))
    columns = [row[0] for row in result.fetchall()]
    print("Kolommen in 'urenregistratie':")
    print(columns)
    
    # Print de eerste 10 waarden van date_date uit urenregistratie
    result = conn.execute(text('''
        SELECT id, date_date FROM urenregistratie LIMIT 10;
    '''))
    df = pd.DataFrame(result.fetchall(), columns=['id', 'date_date'])
    print("Eerste 10 date_date entries uit urenregistratie:")
    print(df.to_string(index=False))
    
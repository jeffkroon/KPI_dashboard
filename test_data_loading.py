import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Laad environment variabelen
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")

engine = create_engine(POSTGRES_URL)

print("=== TEST DATA LOADING ===")

# Test 1: Hoeveel projectlines zijn er totaal?
print("\n1. Totaal projectlines:")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM projectlines_per_company"))
    total_pl = result.fetchone()[0]
    print(f"   Totaal projectlines: {total_pl}")

# Test 2: Hoeveel projectlines met unit='uur'?
print("\n2. Projectlines met unit='uur':")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM projectlines_per_company WHERE unit_searchname ILIKE 'uur'"))
    uren_pl = result.fetchone()[0]
    print(f"   Projectlines met unit='uur': {uren_pl}")

# Test 3: Hoeveel bedrijven hebben projectlines?
print("\n3. Bedrijven met projectlines:")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(DISTINCT bedrijf_id) FROM projectlines_per_company"))
    bedrijven_met_pl = result.fetchone()[0]
    print(f"   Bedrijven met projectlines: {bedrijven_met_pl}")

# Test 4: Hoeveel bedrijven hebben uren?
print("\n4. Bedrijven met uren:")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(DISTINCT bedrijf_id) FROM projectlines_per_company WHERE unit_searchname ILIKE 'uur'"))
    bedrijven_met_uren = result.fetchone()[0]
    print(f"   Bedrijven met uren: {bedrijven_met_uren}")

# Test 5: Korff specifiek
print("\n5. Korff Dakwerken specifiek:")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT c.id, c.companyname, 
               COUNT(pl.id) as total_projectlines,
               COUNT(CASE WHEN pl.unit_searchname ILIKE 'uur' THEN 1 END) as uren_projectlines,
               SUM(CASE WHEN pl.unit_searchname ILIKE 'uur' THEN CAST(pl.amountwritten AS FLOAT) ELSE 0 END) as totaal_uren
        FROM companies c
        LEFT JOIN projectlines_per_company pl ON c.id = pl.bedrijf_id
        WHERE c.companyname ILIKE '%korff%'
        GROUP BY c.id, c.companyname
    """))
    korff_data = result.fetchall()
    for row in korff_data:
        print(f"   - {row[1]} (ID: {row[0]}): {row[2]} projectlines, {row[3]} urenregels, {row[4]} totaal uren")

# Test 6: Vergelijk met directe SQL
print("\n6. Vergelijk met directe SQL:")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT bedrijf_id, SUM(CAST(amountwritten AS FLOAT)) as totaal_uren
        FROM projectlines_per_company 
        WHERE unit_searchname ILIKE 'uur'
        GROUP BY bedrijf_id
        ORDER BY totaal_uren DESC
        LIMIT 10
    """))
    direct_sql = result.fetchall()
    print(f"   Directe SQL resultaat: {len(direct_sql)} rijen")
    for row in direct_sql[:5]:
        print(f"     - Bedrijf ID: {row[0]}, Uren: {row[1]}")

# Test 7: Check of er een LIMIT is in de load_data_df functie
print("\n7. Check load_data_df functie:")
try:
    # Simuleer de exacte query die in projectrendement.py wordt gebruikt
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT bedrijf_id, SUM(CAST(amountwritten AS FLOAT)) as totaal_uren
            FROM projectlines_per_company 
            WHERE unit_searchname ILIKE 'uur'
            GROUP BY bedrijf_id
        """))
        load_data_result = result.fetchall()
        print(f"   Load_data_df simulatie: {len(load_data_result)} rijen")
        
        # Zoek specifiek naar Korff (ID: 95837)
        korff_found = False
        for row in load_data_result:
            if row[0] == 95837:  # Korff ID
                print(f"   ✅ Korff gevonden: Bedrijf ID {row[0]}, Uren: {row[1]}")
                korff_found = True
                break
        
        if not korff_found:
            print(f"   ❌ Korff (ID: 95837) niet gevonden in de resultaten")
        
        print(f"   Eerste 5 rijen:")
        for row in load_data_result[:5]:
            print(f"     - Bedrijf ID: {row[0]}, Uren: {row[1]}")
except Exception as e:
    print(f"   Fout bij load_data_df simulatie: {e}")

# Test 8: Check of Korff de juiste tag heeft
print("\n8. Check Korff tags:")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT id, companyname, tag_names
        FROM companies 
        WHERE id = 95837
    """))
    korff_company = result.fetchone()
    if korff_company:
        print(f"   - ID: {korff_company[0]}")
        print(f"   - Naam: {korff_company[1]}")
        print(f"   - Tags: {korff_company[2]}")
        if "1 | Externe opdrachten / contracten" in str(korff_company[2]):
            print(f"   ✅ Heeft klant tag")
        else:
            print(f"   ❌ Heeft geen klant tag")
    else:
        print(f"   ❌ Korff niet gevonden in companies tabel")

# Test 9: Check facturen van Korff
print("\n9. Check facturen van Korff:")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) as total_facturen,
               COUNT(CASE WHEN fase = 'Factuur' THEN 1 END) as gefactureerde_facturen,
               SUM(CASE WHEN fase = 'Factuur' THEN CAST(totalpayed AS FLOAT) ELSE 0 END) as totaal_gefactureerd
        FROM invoices 
        WHERE company_id = 95837
    """))
    korff_facturen = result.fetchone()
    print(f"   - Totaal facturen: {korff_facturen[0]}")
    print(f"   - Gefactureerde facturen: {korff_facturen[1]}")
    print(f"   - Totaal gefactureerd: €{korff_facturen[2]}")

# Test 10: Simuleer de exacte bedrijfsstats berekening
print("\n10. Simuleer bedrijfsstats berekening:")
with engine.connect() as conn:
    # Stap 1: uren per bedrijf (alleen klant bedrijven)
    result = conn.execute(text("""
        SELECT c.id, c.companyname,
               SUM(CAST(pl.amountwritten AS FLOAT)) as totaal_uren
        FROM companies c
        JOIN projectlines_per_company pl ON c.id = pl.bedrijf_id
        WHERE c.tag_names LIKE '%1 | Externe opdrachten / contracten%'
        AND pl.unit_searchname ILIKE 'uur'
        GROUP BY c.id, c.companyname
        HAVING SUM(CAST(pl.amountwritten AS FLOAT)) > 0
    """))
    uren_result = result.fetchall()
    print(f"   Bedrijven met uren (klant tag): {len(uren_result)}")
    
    # Zoek Korff in deze lijst
    korff_in_uren = False
    for row in uren_result:
        if row[0] == 95837:
            print(f"   ✅ Korff in uren lijst: {row[1]} - {row[2]} uren")
            korff_in_uren = True
            break
    
    if not korff_in_uren:
        print(f"   ❌ Korff niet in uren lijst")
    
    # Stap 2: facturen per bedrijf (alleen klant bedrijven)
    result = conn.execute(text("""
        SELECT c.id, c.companyname,
               SUM(CAST(i.totalpayed AS FLOAT)) as totaal_facturen
        FROM companies c
        JOIN invoices i ON c.id = i.company_id
        WHERE c.tag_names LIKE '%1 | Externe opdrachten / contracten%'
        AND i.fase = 'Factuur'
        GROUP BY c.id, c.companyname
        HAVING SUM(CAST(i.totalpayed AS FLOAT)) > 0
    """))
    facturen_result = result.fetchall()
    print(f"   Bedrijven met facturen (klant tag): {len(facturen_result)}")
    
    # Zoek Korff in deze lijst
    korff_in_facturen = False
    for row in facturen_result:
        if row[0] == 95837:
            print(f"   ✅ Korff in facturen lijst: {row[1]} - €{row[2]}")
            korff_in_facturen = True
            break
    
    if not korff_in_facturen:
        print(f"   ❌ Korff niet in facturen lijst") 
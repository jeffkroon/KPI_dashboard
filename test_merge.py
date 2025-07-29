import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Laad environment variabelen
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    raise ValueError("POSTGRES_URL is not set in the environment.")

engine = create_engine(POSTGRES_URL)

print("=== TEST MERGE LOGICA ===")

# Simuleer de exacte logica uit projectrendement.py
print("\n1. Stap 1: uren_per_bedrijf")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT bedrijf_id, SUM(CAST(amountwritten AS FLOAT)) as totaal_uren
        FROM projectlines_per_company 
        WHERE unit_searchname ILIKE 'uur'
        GROUP BY bedrijf_id
    """))
    uren_data = result.fetchall()
    print(f"   Totaal bedrijven met uren: {len(uren_data)}")
    
    # Filter op klant bedrijven
    result = conn.execute(text("""
        SELECT c.id as bedrijf_id
        FROM companies c
        WHERE c.tag_names LIKE '%1 | Externe opdrachten / contracten%'
    """))
    klant_bedrijven = [row[0] for row in result.fetchall()]
    print(f"   Klant bedrijven: {len(klant_bedrijven)}")
    
    # Filter uren op klant bedrijven
    uren_klanten = [row for row in uren_data if row[0] in klant_bedrijven]
    print(f"   Klant bedrijven met uren: {len(uren_klanten)}")
    
    # Check Korff
    korff_uren = None
    for row in uren_klanten:
        if row[0] == 95837:  # Korff ID
            korff_uren = row
            break
    
    if korff_uren:
        print(f"   ✅ Korff in uren: {korff_uren[1]} uren")
    else:
        print(f"   ❌ Korff niet in uren")

print("\n2. Stap 2: factuurbedrag_per_bedrijf")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT company_id as bedrijf_id, SUM(CAST(totalpayed AS FLOAT)) as totalpayed
        FROM invoices 
        WHERE fase = 'Factuur'
        GROUP BY company_id
    """))
    facturen_data = result.fetchall()
    print(f"   Totaal bedrijven met facturen: {len(facturen_data)}")
    
    # Filter facturen op klant bedrijven
    facturen_klanten = [row for row in facturen_data if row[0] in klant_bedrijven]
    print(f"   Klant bedrijven met facturen: {len(facturen_klanten)}")
    
    # Check Korff
    korff_facturen = None
    for row in facturen_klanten:
        if row[0] == 95837:  # Korff ID
            korff_facturen = row
            break
    
    if korff_facturen:
        print(f"   ✅ Korff in facturen: €{korff_facturen[1]}")
    else:
        print(f"   ❌ Korff niet in facturen")

print("\n3. Stap 3: Merge simulatie")
# Simuleer de merge
uren_dict = {row[0]: row[1] for row in uren_klanten}
facturen_dict = {row[0]: row[1] for row in facturen_klanten}

# Merge logic
merged_bedrijven = set(uren_dict.keys()) | set(facturen_dict.keys())
print(f"   Totaal bedrijven na merge: {len(merged_bedrijven)}")

# Check Korff in merge
if 95837 in merged_bedrijven:
    print(f"   ✅ Korff in merge")
    uren = uren_dict.get(95837, 0)
    facturen = facturen_dict.get(95837, 0)
    print(f"   - Korff uren: {uren}")
    print(f"   - Korff facturen: €{facturen}")
    if uren > 0 and facturen > 0:
        tarief = facturen / uren
        print(f"   - Korff tarief: €{tarief:.2f}/uur")
    else:
        print(f"   - Korff heeft geen geldige tarief (uren: {uren}, facturen: €{facturen})")
else:
    print(f"   ❌ Korff niet in merge")

print("\n4. Stap 4: Final filtering")
# Simuleer de final filtering
final_bedrijven = []
for bedrijf_id in merged_bedrijven:
    uren = uren_dict.get(bedrijf_id, 0)
    facturen = facturen_dict.get(bedrijf_id, 0)
    
    # Handle None values
    if uren is None:
        uren = 0
    if facturen is None:
        facturen = 0
    
    if uren > 0:  # Filter op uren > 0
        if facturen > 0:  # Heeft ook facturen
            tarief = facturen / uren
            if tarief > 0:  # Filter op tarief > 0
                final_bedrijven.append((bedrijf_id, uren, facturen, tarief))

print(f"   Bedrijven na final filtering: {len(final_bedrijven)}")

# Check Korff in final
korff_final = None
for bedrijf_id, uren, facturen, tarief in final_bedrijven:
    if bedrijf_id == 95837:
        korff_final = (bedrijf_id, uren, facturen, tarief)
        break

if korff_final:
    print(f"   ✅ Korff in final: {korff_final[1]} uren, €{korff_final[2]}, €{korff_final[3]:.2f}/uur")
else:
    print(f"   ❌ Korff niet in final")
    
    # Check waarom niet
    uren = uren_dict.get(95837, 0)
    facturen = facturen_dict.get(95837, 0)
    print(f"   - Korff uren: {uren} (moet > 0)")
    print(f"   - Korff facturen: €{facturen} (moet > 0)")
    if uren > 0 and facturen > 0:
        tarief = facturen / uren
        print(f"   - Korff tarief: €{tarief:.2f}/uur (moet > 0)")
    else:
        print(f"   - Korff heeft geen geldige data voor tarief berekening") 
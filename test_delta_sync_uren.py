from gripp_api import fetch_gripp_hours_data_delta, convert_date_columns, safe_to_sql

def test_delta_sync_urenregistratie():
    print("=== TEST: Delta-sync urenregistratie ===")
    df = fetch_gripp_hours_data_delta()
    print(f"Aantal rijen opgehaald: {len(df)}")
    if not df.empty:
        print("Voorbeeld van data:")
        print(df.head())
        # Converteer date kolommen
        df = convert_date_columns(df)
        # Probeer weg te schrijven naar een test-tabel (of comment uit als je alleen wilt ophalen)
        # safe_to_sql(df, 'urenregistratie_test')
    else:
        print("Geen nieuwe of gewijzigde uren gevonden sinds laatste sync.")

if __name__ == "__main__":
    test_delta_sync_urenregistratie()
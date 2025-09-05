import pandas as pd
from utils.data_loaders import load_data_df

# Load companies dataset
df_companies = load_data_df("companies", columns=["id", "companyname"])

# Find the ID of the company 'EV Administratie & Advies B.V.'
target_company = df_companies[df_companies["companyname"] == "EV Administratie & Advies B.V."]
if target_company.empty:
    print("Bedrijf niet gevonden.")
    exit()

company_id = target_company.iloc[0]["id"]
print(f"Bedrijf ID voor 'EV Administratie & Advies B.V.': {company_id}")

# Load projectlines dataset
df_projectlines = load_data_df("projectlines_per_company", columns=["bedrijf_id", "amount", "amountwritten", "sellingprice", "unit_searchname", "description"])
df_projectlines["totalexclvat"] = pd.to_numeric(df_projectlines["sellingprice"], errors="coerce").fillna(0) * pd.to_numeric(df_projectlines["amount"], errors="coerce").fillna(0)

# Filter projectlines for the target company
df_target_projectlines = df_projectlines[df_projectlines["bedrijf_id"] == company_id]

print(f"Aantal projectlines gevonden: {len(df_target_projectlines)}")
print(df_target_projectlines)
import pandas as pd
from utils.data_loaders import load_data_df

def main():
    # Dataframes laden
    df_projects = load_data_df("projects", columns=["id", "name", "company_id", "archived", "totalexclvat", "phase_searchname"])
    df_companies = load_data_df("companies", columns=["id", "companyname"])

    # Filter niet-gearchiveerde projecten in 'Voorbereiding' of 'Uitvoering'
    df_projects_filtered = df_projects[
        (df_projects["archived"] == False) &
        (df_projects["phase_searchname"].isin(["Voorbereiding", "Uitvoering"]))
    ]
    print(f"Aantal projecten na filteren: {len(df_projects_filtered)}")

    # Merge met companies
    df_projects_merged = df_projects_filtered.merge(
        df_companies[['id', 'companyname']],
        left_on='company_id',
        right_on='id',
        how='left',
        suffixes=('', '_company')
    )
    print(f"Aantal projecten na merge: {len(df_projects_merged)}")

    # Extra checks
    print("Aantal unieke projectnamen:", df_projects_merged['name'].nunique())
    print("Aantal unieke project IDs:", df_projects_merged['id_x'].nunique())
    print("Aantal projecten zonder company_id:", df_projects_merged['company_id'].isnull().sum())
    print("Aantal projecten zonder company match:", df_projects_merged['companyname'].isnull().sum())

    # Toon eventueel de eerste paar rijen
    print(df_projects_merged.head())

if __name__ == "__main__":
    main() 
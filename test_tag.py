import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Laad de omgeving
load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")
engine = create_engine(POSTGRES_URL)

# 1. Laad projecten
df_projects = pd.read_sql("SELECT id, name FROM projects WHERE archived = FALSE", engine)

# 2. Laad urenregistraties
df_uren = pd.read_sql("""
    SELECT offerprojectbase_id, employee_id, amount 
    FROM urenregistratie 
    WHERE status_searchname = 'Gefiatteerd'
""", engine)

# 3. Laad projectlines
df_projectlines = pd.read_sql("SELECT offerprojectbase_id FROM projectlines_per_company", engine)

# 4. Groepeer uren per project
uren_per_project = df_uren.groupby('offerprojectbase_id')['employee_id'].nunique().reset_index()
uren_per_project.columns = ['project_id', 'unique_employees']

# 5. Tel projectlines per project
projectlines_per_project = df_projectlines.groupby('offerprojectbase_id').size().reset_index(name='projectlines_count')

# 6. Combineer datasets
df_check = uren_per_project.merge(projectlines_per_project, left_on='project_id', right_on='offerprojectbase_id', how='left').fillna(0)

# 7. Filter: veel medewerkers maar geen projectlines
df_suspect = df_check[(df_check['unique_employees'] >= 3) & (df_check['projectlines_count'] == 0)]

# 8. Output
resultaat = df_suspect.merge(df_projects, left_on='project_id', right_on='id')[['project_id', 'name', 'unique_employees']]
print("\n❗ Verdachte projecten waar veel mensen uren op schrijven maar geen projectregels:\n")
print(resultaat)
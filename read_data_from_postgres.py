import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Loads environmental vars from .env
load_dotenv()

postgres_host = os.environ.get("postgres_host")
postgres_database = os.environ.get("postgres_database")
postgres_user = os.environ.get("postgres_user")
postgres_password = os.environ.get("postgres_password")
postgres_port = os.environ.get("postgres_port")
dest_folder = os.environ.get("dest_folder")
api_url = os.environ.get("api_url")
dataset_id = os.environ.get("dataset_id")
table_id = os.environ.get("table_id")
destination_path = f"{dest_folder}/{dataset_id}.json"
finished_workflow = os.environ.get("finished_workflow")

engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

def read_all_tables_from_postgres(table_id):

    try:
        sql = f'SELECT * FROM "{table_id}";'
        df_crime = pd.read_sql_query(sql, con=engine)
        sql_geo = f'SELECT * FROM "{table_id}_geo";'
        df_geo = pd.read_sql_query(sql_geo, con=engine)
        sql_hour = f'SELECT * FROM "{table_id}_crimes_per_hour";'
        df_hour = pd.read_sql_query(sql_hour, con=engine)
        sql_year = f'SELECT * FROM "{table_id}_crimes_per_year";'
        df_year = pd.read_sql_query(sql_year, con=engine)
        sql_top = f'SELECT * FROM "{table_id}_top_crimes";'
        df_top = pd.read_sql_query(sql_top, con=engine)
    except Exception as e:
        print(f"Error: Tables cannot be read due to: {e}")
    return df_crime, df_geo, df_hour, df_year, df_top


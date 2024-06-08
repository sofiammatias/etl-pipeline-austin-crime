"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
"""
import streamlit as st
from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv
from transform_create_dfs import (
    create_base_df,
    create_df_geo,
    create_crimes_per_hour,
    create_crimes_per_year,
    top_crimes,
)

load_dotenv()

postgres_host = os.environ.get("postgres_host")
postgres_database = os.environ.get("postgres_database")
postgres_user = os.environ.get("postgres_user")
postgres_password = os.environ.get("postgres_password")
postgres_port = os.environ.get("postgres_port")
dest_folder = os.environ.get("dest_folder")
dataset_id = os.environ.get("dataset_id")
table_id = os.environ.get("table_id")

destination_path = f"{dest_folder}/{dataset_id}.csv"

engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

def create_dfs_to_postgres_main():
    
    main_df = pd.DataFrame([])
    main_df, logger_msg = create_base_df(engine)
    if "successfully" in logger_msg:
        df_geo = create_df_geo(main_df)
        df_crimes_per_hour = create_crimes_per_hour(main_df)
        df_crimes_per_year = create_crimes_per_year(main_df)
        df_top_crimes = top_crimes(main_df)

        main_df.to_sql(name = f'{table_id}', con=engine, if_exists='replace')
        df_geo.to_sql(name = f'{table_id}_geo', con=engine, if_exists='replace')
        df_crimes_per_hour.to_sql(name = f'{table_id}_crimes_per_hour', con=engine, if_exists='replace')
        df_crimes_per_year.to_sql(name = f'{table_id}_crimes_per_year', con=engine, if_exists='replace')
        df_top_crimes.to_sql(name = f'{table_id}_top_crimes', con=engine, if_exists='replace')

    return logger_msg

if __name__ == "__main__":

    main_df = create_base_df(engine)
    df_geo = create_df_geo(main_df)
    df_crimes_per_hour = create_crimes_per_hour(main_df)
    df_crimes_per_year = create_crimes_per_year(main_df)
    df_top_crimes = top_crimes(main_df)

    df_geo.to_sql(name = f'{table_id}_geo', con=engine, if_exists='replace')
    df_crimes_per_hour.to_sql(name = f'{table_id}_crimes_per_hour', con=engine, if_exists='replace')
    df_crimes_per_year.to_sql(name = f'{table_id}_crimes_per_year', con=engine, if_exists='replace')
    df_top_crimes.to_sql(name = f'{table_id}_top_crimes', con=engine, if_exists='replace')

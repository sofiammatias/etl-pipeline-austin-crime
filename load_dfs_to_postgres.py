"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
"""
import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
from transform_create_dfs import (
    create_base_df,
    create_df_geo,
    create_crimes_per_hour,
    create_crimes_per_year,
    top_crimes,
)

postgres_host = st.secrets.postgres_host
postgres_database = st.secrets.postgres_database
postgres_user = st.secrets.postgres_user
postgres_password = st.secrets.postgres_password
postgres_port = st.secrets.postgres_port
dest_folder = st.secrets.dest_folder
api_url = st.secrets.api_url
dataset_id = st.secrets.dataset_id
table_id = st.secrets.table_id
destination_path = f"{dest_folder}/{dataset_id}.json"

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

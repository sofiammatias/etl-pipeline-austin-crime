"""
NEW: Downloads a json file from Austin Crime website API datapoint. 
Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
from sqlalchemy import create_engine
import streamlit as st
import os
import pandas as pd
import requests
import json


# Loads environmental vars from secrets.toml

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

def download_json_file_from_url(api_url: str, dest_folder: str, destination_path: str):
    """
    Download Austin crime dataset from API endpoint: https://data.austintexas.gov/resource/fdj4-gpfu.json
    Save data to postgresql database.
    """

    if not os.path.exists(str(dest_folder)):
        os.makedirs(str(dest_folder))  # create folder if it does not exist

    try:
        data = requests.get(api_url)
        json_file = data.json()
        with open(destination_path, "w") as file:
            json.dump(json_file, file)
        logger_msg = f"json file downloaded successfully to the working directory {dest_folder}"
    
    except Exception as e:
        logger_msg = f"Error while downloading the json file due to: {e}"

    return logger_msg

def write_to_postgres(destination_path: str):
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df_aux = pd.read_json(f"{destination_path}")
    df = df_aux.reindex(
        columns=[
            "incident_report_number",
            "address",
            "census_tract",
            "clearance_date",
            "clearance_status",
            "council_district",
            "category_description",
            "district",
            "location_type",
            "crime_type",
            "family_violence",
            "occ_date",
            "rep_date_time",
            "latitude",
            "longitude",
            "year",
            "zipcode",
        ]
    )
    df.to_sql(name = f'{table_id}', con=engine, if_exists='replace', index=False)


def write_json_to_postgres_main():
    logger_msg1 = download_json_file_from_url(api_url, dest_folder, destination_path)
    if "successfully" in logger_msg1:
        write_to_postgres(destination_path)
        logger_msg2 = f"Table '{table_id}' created in postgreSQL with success"
    return logger_msg1, logger_msg2
    


if __name__ == "__main__":
    logger_msg1 = download_json_file_from_url(api_url, dest_folder, destination_path)
    print (logger_msg1)
    if "successfully" in logger_msg1:
        write_to_postgres(destination_path)
        logger_msg2 = f"{table_id} created in postgreSQL with success"
        print (logger_msg2)
    


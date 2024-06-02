"""
NEW: Downloads a json file from Austin Crime website API datapoint. 
Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import streamlit as st
import psycopg2
import os
import traceback2 as traceback
import logging
import pandas as pd
import requests
import json
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)


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

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port,
    )
    cur = conn.cursor()
    logging.info("Postgres server connection is successful")
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")


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
        logging.info(
            f"json file downloaded successfully to the working directory {dest_folder}")
        st.toast (f"json file downloaded successfully to the working directory {dest_folder}")
    
    except Exception as e:
        logging.error(f"Error while downloading the json file due to: {e}")
        traceback.print_exc()
        st.toast (f"json file downloaded successfully to the working directory {dest_folder}")



def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_id} (incident_report_number BIGINT PRIMARY KEY, 
                    address VARCHAR(100), census_tract FLOAT, clearance_date VARCHAR(50), 
                    clearance_status VARCHAR(50), council_district FLOAT, category_description VARCHAR(100), 
                    district VARCHAR(50), location_type VARCHAR(50), crime_type VARCHAR(100), 
                    family_violence VARCHAR(100), occ_date_time VARCHAR(50), x_coordinate FLOAT, 
                    y_coordinate FLOAT, year FLOAT, zipcode VARCHAR(50))"""
        )

        logging.info(
            f" New table {table_id} created successfully in postgres server, database {postgres_database}"
        )
        st.toast (f" New table {table_id} created successfully in postgres server, database {postgres_database}")
    except:
        logging.warning(f" Check if the table {table_id} exists")


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
            "occ_date_time",
            "x_coordinate",
            "y_coordinate",
            "year",
            "zipcode",
        ]
    )
    inserted_row_count = 0
    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM {table_id} WHERE incident_report_number = {row['incident_report_number']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute(
                f"""INSERT INTO {table_id} (incident_report_number, address, census_tract, clearance_date, clearance_status, council_district, category_description, district, location_type, crime_type, family_violence, occ_date_time, x_coordinate, y_coordinate, year, zipcode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    int(row[0]),
                    str(row[1]),
                    float(row[2]),
                    str(row[3]),
                    str(row[4]),
                    (row[5]),
                    str(row[6]),
                    str(row[7]),
                    str(row[8]),
                    str(row[9]),
                    str(row[10]),
                    str(row[11]),
                    float(row[12]),
                    float(row[13]),
                    float(row[14]),
                    str(row[15]),
                ),
            )

    logging.info(
        f" {inserted_row_count} rows from csv file inserted into {table_id} table successfully"
    )
    st.toast (f" {inserted_row_count} rows from csv file inserted into {table_id} table successfully")


def write_json_to_postgres_main():
    download_json_file_from_url(api_url, dest_folder, destination_path)
    create_postgres_table()
    write_to_postgres(destination_path)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Connection to {dataset_id} was closed successfully")


if __name__ == "__main__":
    download_json_file_from_url(api_url, dest_folder, destination_path)
    create_postgres_table()
    write_to_postgres(destination_path)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Connection to {dataset_id} was closed successfully")

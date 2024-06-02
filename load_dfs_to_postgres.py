"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
"""
import streamlit as st
import psycopg2
import os
import traceback
import logging
import pandas as pd
from dotenv import load_dotenv
from transform_create_dfs import (
    create_base_df,
    create_df_geo,
    create_crimes_per_hour,
    create_crimes_per_year,
    top_crimes,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
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


def create_new_tables_in_postgres(table_id):
    try:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_id}_geo (crime_type VARCHAR(50), district VARCHAR(10), latitude FLOAT, longitude FLOAT)"
        )
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_id}_crimes_per_hour (hourday INTEGER, numbercrimes INTEGER)"
        )
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_id}_crimes_per_year (year INTEGER, numbercrimes INTEGER)"
        )
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_id}_top_crimes  (crime VARCHAR(100), numbercrimes INTEGER)"
        )
        logging.info("4 tables created successfully in Postgres server")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Tables cannot be created due to: {e}")


def insert_geo_table(df_geo, table_id):
    query = f"INSERT INTO {table_id}_geo (crime_type, district, latitude, longitude) VALUES (%s,%s,%s,%s)"
    row_count = 0
    for _, row in df_geo.iterrows():
        values = (
            row["crime_type"],
            row["district"],
            row["x_coordinate"],
            row["y_coordinate"],
        )
        cur.execute(query, values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table {table_id}_geo")


def insert_crimes_per_hour_table(df_crimes_per_hour, table_id):
    query = (
        f"INSERT INTO {table_id}_crimes_per_hour (hourday, numbercrimes) VALUES (%s,%s)"
    )
    row_count = 0
    for _, row in df_crimes_per_hour.iterrows():
        values = (row["hour"], row["number_of_crimes"])
        cur.execute(query, values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table {table_id}_crimes_per_hour")


def insert_crimes_per_year_table(df_crimes_per_year, table_id):
    query = (
        f"INSERT INTO {table_id}_crimes_per_year (year, numbercrimes) VALUES (%s,%s)"
    )
    row_count = 0
    for _, row in df_crimes_per_year.iterrows():
        values = (row["year"], row["number_of_crimes"])
        cur.execute(query, values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table {table_id}_crimes_per_year")


def insert_top_crimes_table(df_top_crimes, table_id):
    query = f"INSERT INTO {table_id}_top_crimes (crime, numbercrimes) VALUES (%s,%s)"
    row_count = 0
    for _, row in df_top_crimes.iterrows():
        values = (row["crime_type"], row["number_of_crimes"])
        cur.execute(query, values)
        row_count += 1

    logging.info(f"{row_count} rows inserted into table {table_id}_top_crimes")
    st.toast (f"{row_count} rows inserted into table {table_id}_top_crimes")

def create_dfs_to_postgres_main():
    main_df = create_base_df(cur)

    df_geo = create_df_geo(main_df)
    df_crimes_per_hour = create_crimes_per_hour(main_df)
    df_crimes_per_year = create_crimes_per_year(main_df)
    df_top_crimes = top_crimes(main_df)

    create_new_tables_in_postgres(table_id)
    insert_geo_table(df_geo, table_id)
    insert_crimes_per_hour_table(df_crimes_per_hour, table_id)
    insert_crimes_per_year_table(df_crimes_per_year, table_id)
    insert_top_crimes_table(df_top_crimes, table_id)

    conn.commit()
    cur.close()
    conn.close()

    logging.info(
        f"Tables loaded to postgresql database. Connection to {dataset_id} was closed successfully"
    )


if __name__ == "__main__":
    main_df = create_base_df(cur)

    df_geo = create_df_geo(main_df)
    df_crimes_per_hour = create_crimes_per_hour(main_df)
    df_crimes_per_year = create_crimes_per_year(main_df)
    df_top_crimes = top_crimes(main_df)

    create_new_tables_in_postgres(table_id)
    insert_geo_table(df_geo, table_id)
    insert_crimes_per_hour_table(df_crimes_per_hour, table_id)
    insert_crimes_per_year_table(df_crimes_per_year, table_id)
    insert_top_crimes_table(df_top_crimes, table_id)

    conn.commit()
    cur.close()
    conn.close()

    logging.info(
        f"Tables loaded to postgresql database. Connection to {dataset_id} was closed successfully"
    )
    st.toast (f"Tables loaded to postgresql database. Connection to {dataset_id} was closed successfully")
import streamlit as st
from prefect import task, flow 
import pandas as pd
import numpy as np
import time
import os
import psycopg2
import traceback
import subprocess
import requests
import json
from dotenv import load_dotenv


st.set_page_config(page_title='ETL Pipeline' ,layout="wide",page_icon='🔁')

# Some functions

def card(wch_colour_box, wch_colour_font, sline, i, iconname=None):
      """Displays a nice colored card"""
      fontsize = 24
      valign = "center"
      lnk = '<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.12.1/css/all.css" crossorigin="anonymous">'

      htmlstr = f"""<p style='background-color: rgb({wch_colour_box[0]}, 
                                                  {wch_colour_box[1]}, 
                                                  {wch_colour_box[2]}, 0.75); 
                            color: rgb({wch_colour_font[0]}, 
                                      {wch_colour_font[1]}, 
                                      {wch_colour_font[2]}); 
                            font-size: {fontsize}px;
                            text-align: {valign};
                            border-radius: 7px; 
                            padding-left: 12px; 
                            padding-top: 18px; 
                            padding-bottom: 18px; 
                            line-height:25px;'>
                            <i class='{iconname}'></i> {i}
                            </style><BR><span style='font-size: 14px; 
                            margin-top: 0;'>{sline}</style></span></p>"""
      return lnk + htmlstr

def read_tables_from_postgres(table_id):
    
    try:
        sql = f"SELECT * FROM {table_id};"
        df_crime = pd.read_sql_query(sql, conn)
        sql_geo = f"SELECT * FROM {table_id}_geo;"
        df_geo = pd.read_sql_query(sql_geo, conn)
        sql_hour = f"SELECT * FROM {table_id}_crimes_per_hour;"
        df_hour = pd.read_sql_query(sql_hour, conn)
        sql_year = f"SELECT * FROM {table_id}_crimes_per_year;"
        df_year = pd.read_sql_query(sql_year, conn)
        sql_top = f"SELECT * FROM {table_id}_top_crimes;"
        df_top = pd.read_sql_query(sql_top, conn)
    except Exception as e:
        st.write(f"❗Error: Tables cannot be read due to: {e}")
    return df_crime, df_geo, df_hour, df_year, df_top


# Loads environmental vars from .env
load_dotenv()
finished_flow=False

st.title("ETL Pipeline - Austin Crime Database 👮‍♂️")

# Add buttons to sidebar

show_workflow_button = st.sidebar.button ("See How Pipeline Works", type="primary")
start_workflow_button = st.sidebar.button ("Start Pipeline")

pipeline, see_data, dashboard = st.tabs (["Pipeline", "Data", "Dashboard"])

with pipeline:
    col1, _, col3 = st.columns([4,1,4])
    
    # App explanation in main page
    with col1:
        with st.expander ("How to use this app:"):
            st.markdown (
            """  - Press the button **'See How Pipeline Works'** to know the pipeline various steps  \n  - Press
            the button **'Start Pipeline'** to run the pipeline  \n  - See the tab 'Data' to verify what sort 
            of data the pipeline retrieved  \n  - See the tab 'Dashboard' to visualize the data from the data 
            warehouse tables""")

        st.markdown(
            """This project simulates a small **ETL automated data pipeline**, using *Prefect*. ETL stands for 
            Extract-Transform-Load and in this app you can see how data goes through those steps:  \n  - Download 
            data from https://data.austintexas.gov/ API as json file - Extract  \n  - Creation of postgreSQL 
            database to feed it with the .json file's info - Extract  \n  - Creation of relevant dataframes 
            from the original table (data warehouse) - Transform  \n  - Load dataframes into the PostgreSQL 
            database - Load  \n  - Creation of a dashboard (we want to see the data, right?)""")


    # Shows how pipeline works when button is pressed

    if show_workflow_button:
        with col3:
            st.subheader("How Does The Pipeline Work?")
            st.markdown(card((46, 216, 182),(255,255,255), ".json file", "Download json file from API"), unsafe_allow_html=True)
            with st.spinner('Opening API site...'):
                time.sleep(3)
            with st.spinner('Loading json file...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((255, 182, 77),(255,255,255), "PostgreSQL database", "Create database and load .json into tables"), unsafe_allow_html=True)
            with st.spinner('Creating database...'):
                time.sleep(3)
            with st.spinner('Loading tables...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((255, 83, 112),(255,255,255), "PostgreSQL", "Transform data and create data warehouse"), unsafe_allow_html=True)
            with st.spinner('Cleaning data...'):
                time.sleep(3)
            with st.spinner ('Formmating data...'):
                time.sleep(3)
            with st.spinner ('Performing calculations...'):
                time.sleep(3)
            with st.spinner ('Loading new tables into PostgreSQL DB...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((64, 153, 255),(255,255,255), "Dashboard", "Create visuals with new tables"), unsafe_allow_html=True)
        
    if start_workflow_button:
            with col3:
                st.subheader("Pipeline Status")
                with st.status ("Running Pipeline...", expanded=True, state='running') as status:
                    st.write ("Connecting with API...")
                    time.sleep(3)
                    st.write ("Creating database...")
                    # Get env variables for connections/credentials info
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

                    # open connection with Postgresql
                    try:
                        conn = psycopg2.connect(
                            host=postgres_host,
                            database=postgres_database,
                            user=postgres_user,
                            password=postgres_password,
                            port=postgres_port,
                        )
                        cur = conn.cursor()
                        st.write("PostgreSQL server connection ok")

                    except Exception as e:
                        traceback.print_exc()
                        st.write("❗Error: Couldn't create the PostgreSQL connection")


                    # Start pipeline workflow
                    st.write ("Downloading .json file...")
                    try:
                        data = requests.get(api_url)
                        json_file = data.json()
                        with open(destination_path, "w") as file:
                            json.dump(json_file, file)
                        st.write(f".json file downloaded successfully to the working directory {dest_folder}")
        
                    except Exception as e:
                        st.write(f"❗Error while downloading .json file due to {e})")
                        traceback.print_exc()

                    st.write ("Loading data from .json file into PostgreSQL database...")
                    time.sleep(6)
                    st.write ("Transforming data: creating 4 tables...")
                    subprocess.run(["python", "etl-workflow.py"])
                    st.write ("Tables loaded in database")
                    status.update(label="Pipeline is complete!", state="complete", expanded=True)
                    finished_flow = True

with see_data:
    if finished_flow:
        df_crimes, df_geo, df_hour, df_year, df_top_crimes = read_tables_from_postgres(table_id)
        with st.expander (f"See the lastest data retrieved from the austin crime API"):
            df_crimes_no_nan = df_crimes[df_crimes['occ_date_time'].notnull()]
            st.dataframe (df_crimes_no_nan.head())
        with st.expander (f"See the data from the original table  {table_id} created with the .json file"):
            st.dataframe (df_crimes)
        with st.expander (f"See the data of 1st table with transformed data: {table_id}_geo"):
            st.dataframe (df_geo)
        with st.expander (f"See the data of 2nd table with transformed data: {table_id}_crimes_by_hour"):
            st.dataframe (df_hour)
        with st.expander (f"See the data of 3rd table with transformed data: {table_id}_crimes_by_year"):
            st.dataframe (df_year)
        with st.expander (f"See the data of 4th table with transformed data: {table_id}_top_crimes"):
            st.dataframe (df_top_crimes)
    elif not(finished_flow):
        st.info ("There's no data to be seen")


# command to delete json file
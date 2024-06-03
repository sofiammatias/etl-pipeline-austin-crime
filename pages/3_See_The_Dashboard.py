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

# initializing some vars
finished_flow=False

# Sidebar

# Building app
st.title("ETL Pipeline - Austin Crime Database 👮‍♂️")

pipeline, see_data, dashboard = st.tabs (["Pipeline", "Data", "Dashboard"])

with pipeline:
    col1, _, col3 = st.columns([5,1,4])
    
    # App explanation in main page
    with col1:
        with st.expander ("How to use this app:"):
            st.markdown (
            """  - Press **'See How Pipeline Works'** to know what the pipeline does \n  - Press **'Start 
            Pipeline'** to run the pipeline  \n  - Check tab 'Data' to see the postgre tables produced by the 
            pipeline  \n  - Check tab 'Dashboard' to visualize the data from the final tables 
            warehouse tables""")

        st.markdown(
            """This project simulates a small **ETL automated data pipeline**, using *Prefect*. ETL stands for 
            Extract-Transform-Load and in this app you can see how data goes through those steps:  \n  - Download 
            data from https://data.austintexas.gov/ API as json file - Extract  \n  - Creation of postgreSQL 
            database to feed it with the .json file's info - Extract  \n  - Creation of relevant dataframes 
            from the original table, a small data warehouse - Transform  \n  - Load dataframes into the PostgreSQL 
            database - Load  \n  - Creation of a dashboard (we want to see the data, right?)""")

        # Add buttons
        show_workflow_button = st.button ("See How Pipeline Works", type="primary")
        start_workflow_button = st.button ("Start Pipeline")


    # Shows how pipeline works when button is pressed

    if show_workflow_button:
        with col3:
            st.subheader("How Does The Pipeline Work?")
            st.markdown(card((46, 216, 182),(255,255,255), "EXTRACT: a .json file", "Download json file from API"), unsafe_allow_html=True)
            with st.spinner('Opening API site...'):
                time.sleep(3)
            with st.spinner('Loading json file...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((255, 182, 77),(255,255,255), "EXTRACT: to PostgreSQL database", "Create database and load .json into a table"), unsafe_allow_html=True)
            with st.spinner('Creating database...'):
                time.sleep(3)
            with st.spinner('Loading table...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((255, 83, 112),(255,255,255), "TRANSFORM: Pandas/Numpy", "Transform data: clean, format, apply formulas"), unsafe_allow_html=True)
            with st.spinner('Cleaning data...'):
                time.sleep(3)
            with st.spinner ('Formmating data...'):
                time.sleep(3)
            with st.spinner ('Performing calculations...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((64, 153, 255),(255,255,255), "LOAD: PostgreSQL", "Load final tables into DB and create data warehouse"), unsafe_allow_html=True)
            with st.spinner ('Loading new tables into PostgreSQL DB...'):
                time.sleep(3)
            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
            time.sleep(0.5)
            st.markdown(card((46, 216, 182),(255,255,255), "Visualize: Dashboard", "Create visuals with new tables"), unsafe_allow_html=True)
        
    if start_workflow_button:
            with col3:
                st.subheader("Pipeline Status")
                with st.status ("Running Pipeline...", expanded=True, state='running') as status:
                    st.write ("Connecting with API...")
                    time.sleep(3)
                    st.write ("Creating database...")
                    # check if connection with Postgresql is sucessful
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


                    # Download 
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

                    # Start pipeline workflow
                    st.write ("Transforming data: creating 4 tables...")
                    subprocess.run(["python", "etl-workflow.py"])
                    st.write ("Tables loaded in database")
                    status.update(label="Pipeline is complete!", state="complete", expanded=True)
                    finished_flow = True

with see_data:
    col_d1, _, col_d3 = st.columns([5,1,4])
    
    # Explanation of what can be seen in "Data" tab    
    with col_d1:
        with st.expander("What can you see here:"):
            st.write (f" - The *data* local folder contents  \n  - The latest 5 records retrieved (date/time should be very close to local current time)  \n  - The content of the .json file 'as is' in {table_id}  \n - The content of the 4 final tables: {table_id}_geo, {table_id}_by_hour, {table_id}_by_year and {table_id}_top_crimes")
        # Explanation of what can be seen in "Data" tab
        st.write("""The pipeline will retrieve a .json file from https://data.austintexas.gov/ API and save 
                it into local folder *data*. There will also be a *log* file in *data* local folder, reporting all 
                pipeline steps. The pipeline will then create 5 tables into a postgreSQL database: a table with the 
                original data from the .json file and 4 final tables with required data for analysis as well as some 
                aggregation calculations""")

    with col_d3:
        st.write ("Contents of *data* folder:")
        with st.container(border=True):
            for f in os.listdir("./data/"):
                if len(f) == 0:
                    st.write ("(empty)")
                else:
                    st.write (f)
            if st.button ("Delete .json file"):
                if os.path. exists(destination_path):
                    os.remove (destination_path)    

    # Pipeline contents
    if finished_flow:
        df_crimes, df_geo, df_hour, df_year, df_top_crimes = read_tables_from_postgres(table_id)
        with st.expander (f"Latest 5 records"):
            df_crimes_no_nan = df_crimes.loc[df_crimes['occ_date_time'] != np.datetime64('NaT')]
            st.dataframe (df_crimes_no_nan.head())
        with st.expander (f"Table {table_id} created with the .json file"):
            st.dataframe (df_crimes)
        
        scol1, scol2, scol3, scol4 = st.columns(4)
        with scol1:
            with st.expander (f"Table {table_id}_geo"):
                st.dataframe (df_geo)
        with scol2:
            with st.expander (f"Table {table_id}_crimes_by_hour"):
                st.dataframe (df_hour)
        with scol3:
            with st.expander (f"Table {table_id}_crimes_by_year"):
                st.dataframe (df_year)
        with scol4:
            with st.expander (f"Table {table_id}_top_crimes"):
                st.dataframe (df_top_crimes)
    elif not(finished_flow):
        st.info ("There's no data to be seen. Run 'Start Pipeline' button first, in 'Pipeline' tab.")

        


# command to delete json file
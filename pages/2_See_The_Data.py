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
finished_workflow = os.environ.get("finished_workflow")

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port,
    )
    cur = conn.cursor()
    #logging.info("Postgres server connection is successful")
except Exception as e:
    traceback.print_exc()
    #logging.error("Couldn't create the Postgres connection")


# Building app
st.title("ETL Pipeline - Austin Crime Database 👮‍♂️")

with st.expander ("How to use this app:"):
    st.markdown (
    """ 
    **SEE THE DATA** :  \n - See the downloaded .json file in the box, showing the content of the *data* folder.
    Delete it to see the pipeline uploading the file again  \n  - See the date of the latest records, and compare 
    it with your current date and time. The pipeline is downloading the lastest datafrom the API  \n - Choose 
    a table from the dropdown list to visualize the tables in the database""")

# Explanation of what can be seen in "Data" tab    
st.write("""The pipeline retrieves a .json file from https://data.austintexas.gov/ API and saves 
        it into local folder *data*. The pipeline then creates 5 tables into a postgreSQL database: a table with the 
        original data from the .json file and 4 final tables with required data for analysis as well as some 
        aggregation calculations""")

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

st.write(finished_workflow)

# Pipeline contents
if finished_workflow == 'true':
    df_crimes, df_geo, df_hour, df_year, df_top_crimes = read_tables_from_postgres(table_id)
    options_dict = {'json': df_crimes, '_geo': df_geo, '_hour': df_hour, '_year': df_year, '_top': df_top_crimes}
    option = st.selectbox("Choose a table from postgreSQL database:",
    (f"Table {table_id} from json", f"Table {table_id}_geo", f"Table {table_id}_crimes_by_hour",f"Table {table_id}_crimes_by_year", f"Table {table_id}_top_crimes"))
    for key in options_dict.keys():
        if key in option:
            st.dataframe(options_dict[key])
    with st.expander (f"Latest 5 records"):
        df_crimes_no_nan = df_crimes.loc[df_crimes['occ_date_time'] != np.datetime64('NaT')]
        st.dataframe (df_crimes_no_nan.head())
  
elif finished_workflow == 'false':
    st.info ("There's no data to be seen. Run 'Start Pipeline' button first, in 'See The Pipeline'")

    


# command to delete json file
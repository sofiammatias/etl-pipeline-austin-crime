"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
"""
import psycopg2
import os
import traceback
import logging
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

load_dotenv()

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')
dest_folder = os.environ.get('dest_folder')
dataset_id = os.environ.get('dataset_id')
table_id = os.environ.get('table_id')

destination_path = f'{dest_folder}/{dataset_id}.csv' 

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")



def create_base_df(cur):
    """
    Get base dataframe of Austin crime public dataset
    """
    try:
        cur.execute(f'SELECT * FROM {table_id}') 
    except:
        logging.warning(f' Check if the table {table_id} exists')
        return

    rows = cur.fetchall()

    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    # Clean data 
    df.rename(columns = {'occ_date_time': 'occurred_date'}, inplace = True)
    
    # Create parameters
    df_aux = df[['occurred_date','clearance_date']].dropna()
    avg_clearance_time = pd.DataFrame(pd.to_datetime(df_aux['clearance_date']) - pd.to_datetime (df_aux['occurred_date'])).mean()
    df['clearance_date'].fillna(value = avg_clearance_time, inplace = True)
    df['occurred_date'] = pd.to_datetime(df['occurred_date'])
    df['clearance_date'] = pd.to_datetime(df['clearance_date'])
    logging.info(f' Table {table_id} loaded successfully into dataframe from database {postgres_database}')
    return df


def create_df_geo(df):
    """
    Create dataframe from Austin crime public dataset with longitude and latitude data
    """
    df_geo = df.dropna(subset=['x_coordinate', 'y_coordinate'])
    return df_geo
    

def create_crimes_per_hour(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    crimes_per_hour = df['occurred_date'].dt.hour.value_counts().sort_index()
    df_crimes_per_hour = crimes_per_hour.reset_index()
    df_crimes_per_hour.columns=['hour', 'number_of_crimes']
    return df_crimes_per_hour


def create_crimes_per_year(df):
    """
    Create dataframe with number of crimes per year from Austin crime public dataset
    """
    crimes_per_year = df['occurred_date'].dt.year.value_counts().sort_index()
    df_crimes_per_year = crimes_per_year.reset_index()
    df_crimes_per_year.columns=['year', 'number_of_crimes']
    return df_crimes_per_year


def top_crimes(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    df_top_crimes = df['crime_type'].value_counts().head(25).reset_index()
    df_top_crimes.columns = ['crime_type', 'number_of_crimes']
    return df_top_crimes

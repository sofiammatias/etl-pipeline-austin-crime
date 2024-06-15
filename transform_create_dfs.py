"""
Reads the Postgres table as a dataframe and creates 4 separate dataframes from main table. 
"""
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd


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

def create_base_df(engine):
    """
    Get base dataframe of Austin crime public dataset
    """
    try:
        sql = f'SELECT * FROM "{table_id}";'
        df = pd.read_sql_query(sql, con=engine)
        logger_msg = f"Table {table_id} loaded successfully from postgresql"
    except Exception as e:
        logger_msg = f"Error reading table {table_id} from postgresql due to: {e}"

    # Clean data
    df.rename(columns={"occ_date": "occurred_date"}, inplace=True)
    df.rename(columns={"rep_date_time": "reported_time"}, inplace=True)

    #### Filling nan with average clearance time intervals
    most_common_clearance_date = df["clearance_date"].value_counts().index[1]
    #most_common_clearance_date = datetime.strptime(most_common_clearance_date, '%m/%d/%y %H:%M:%S')
    df[df['clearance_date'].isin(['nan'])]['clearance_date'] = most_common_clearance_date
    df["occurred_date"] = pd.to_datetime(df["occurred_date"])
    df["reported_time"] = pd.to_datetime(df["reported_time"])
    df["clearance_date"] = pd.to_datetime(df["clearance_date"])
    return df, logger_msg


def create_df_geo(df):
    """
    Create dataframe from Austin crime public dataset with longitude and latitude data
    """
    df_geo = df[['crime_type', 'district', 'latitude', 'longitude']]
    df_geo = df_geo.dropna(subset=["latitude", "longitude"])
    df_geo.drop(df_geo[df_geo['latitude'] > 32].index, inplace = True)
    df_geo.drop(df_geo[df_geo['latitude'] < 28].index, inplace = True)
    df_geo.drop(df_geo[df_geo['longitude'] > -95].index, inplace = True)
    df_geo.drop(df_geo[df_geo['longitude'] < -99].index, inplace = True)
    return df_geo


def create_crimes_per_hour(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    crimes_per_hour = df["reported_time"].dt.hour.value_counts().sort_index()
    df_crimes_per_hour = crimes_per_hour.reset_index()
    df_crimes_per_hour.columns = ["hour", "number_of_crimes"]
    return df_crimes_per_hour


def create_crimes_per_year(df):
    """
    Create dataframe with number of crimes per year from Austin crime public dataset
    """
    crimes_per_year = df["occurred_date"].dt.year.value_counts().sort_index()
    df_crimes_per_year = crimes_per_year.reset_index()
    df_crimes_per_year.columns = ["year", "number_of_crimes"]

    return df_crimes_per_year


def top_crimes(df):
    """
    Create dataframe with number of crimes per hour of the day from Austin crime public dataset
    """
    df_top_crimes = df["crime_type"].value_counts().head(25).reset_index()
    df_top_crimes.columns = ["crime_type", "number_of_crimes"]
    df_top_crimes = df_top_crimes.groupby(["crime_type"]).sum()

    return df_top_crimes

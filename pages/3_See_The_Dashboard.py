import streamlit as st
from prefect import task, flow 
import json
import pydeck as pdk
import pandas as pd
import numpy as np
import os
import psycopg2
import traceback
from dotenv import load_dotenv
from read_data_from_postgres import read_all_tables_from_postgres

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
finished_workflow = os.environ.get("finished_workflow")
destination_path = f"{dest_folder}/{dataset_id}.json"

# Building app
st.title("ETL Pipeline - Austin Crime Database 👮‍♂️")

    # App explanation in main page
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

except Exception as e:
    traceback.print_exc()
    st.write("❗Error: Couldn't create the PostgreSQL connection")


_, df_geo, df_hour, df_year, df_top_crimes = read_all_tables_from_postgres(table_id)

st.bar_chart (df_hour)
st.map(df_geo)

# Define a layer to display on a map
#layer = pdk.Layer(
#    "ScatterplotLayer",
#    df_geo.head(),
#    pickable=True,
#    opacity=0.8,
#    stroked=True,
#    filled=True,
#    auto_highlight=True,
#    extruded=True,
#    elevation_scale=0.1,
#    elevation_range=[0, 1],
#    radius=1,
#    radius_scale=1,
#    radius_min_pixels=1,
#    radius_max_pixels=100,
#    line_width_min_pixels=1,
#    get_position=["longitude", "latitude"],
#    get_radius=8,
#    get_fill_color=[102, 179, 72],
#    get_line_color=[0, 0, 0],
#)

# Set the viewport location
#view_state = pdk.ViewState(
#    latitude=-23.562799, longitude=-46.663020, zoom=14, bearing=0, pitch=0
#)

# Render
#r = pdk.Deck(
#    layers=[layer],
#    initial_view_state=view_state,
#    map_style="road",
#    tooltip={"text": "{name}\n{address}"},
#)

import streamlit as st
import matplotlib.pyplot as plt
import os
from sqlalchemy import create_engine
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

if finished_workflow == 'false':
    st.info ("There's no data to be seen. Run 'Start Pipeline' button first, in 'See The Pipeline'")
else:
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

    df_crimes, df_geo, df_hour, df_year, df_top_crimes = read_all_tables_from_postgres(table_id)
    df_top_crimes = df_top_crimes.sort_values('number_of_crimes', ascending=False)[:10]

    col1, col2 = st.columns(2)
    fontsize=5

    with col1:
        st.subheader('Number of Crimes by Hour of Day')
        st.bar_chart (df_hour, x="hour", y="number_of_crimes")
        fig1, ax1 = plt.subplots()  
        labels = df_top_crimes['crime_type'].values.tolist()
        ax1.pie (df_top_crimes['number_of_crimes'].values.tolist())
        fig1.patch.set_facecolor("#0E1117")
        fig1.legend(labels=labels, fontsize=fontsize)
        st.subheader('Number of Crimes by Crime Type (Top10)')
        st.pyplot(fig1) 


    with col2:
        st.subheader('Number of Crimes per Year')
        st.bar_chart (df_year, x="year", y="number_of_crimes", color=['#cf3251'])

        df_location = df_crimes[['location_type']].value_counts('location_type', sort=True, ascending=False)[:10]
        fig2, ax2 = plt.subplots()  
        labels = df_location.index.tolist()
        ax2.pie (df_location.values.tolist())
        fig2.patch.set_facecolor("#0E1117")
        fig2.legend(labels=labels, fontsize=fontsize)
        st.subheader('Number of Crimes by Location Type (Top10)')
        st.pyplot(fig2) 


    st.subheader('Crime Density Within Austin City')
    st.map(df_geo)
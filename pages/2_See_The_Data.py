import streamlit as st
import os
from sqlalchemy import create_engine, text
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
destination_path = f"{dest_folder}/{dataset_id}.json"
finished_workflow = os.environ.get("finished_workflow")

engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

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
with st.container(border=True):  
    st.write("""The pipeline retrieves a .json file from https://data.austintexas.gov/ API and saves 
         it into local folder *data*. The pipeline then creates 5 tables into a postgreSQL database: a detailed 
         table with data from the .json file and 4 final tables with aggragated / resumed data for analysis. You 
         can check all the raw data in this page: the .json file in its working folder and the tables from the 
         postgreSQL.""")
    st.write("""🤔 But what if the files and database are all local and the data pipeline doesn't exist?  Make 
         sure the flow is really working by **deleting** both the .json file and the postgres 
         tables!  \n  ⏩⏩⏩ Run the pipeline again in 'See The Pipeline' and come back here to see the new 
         .json file and tables again!  \n  Check out the dates of the latest 5 records and see how close they 
         are from today! 📆""")

# Pipeline contents
if finished_workflow == 'true':

    col1, col2 = st.columns([3,1])
    df_crimes, df_geo, df_hour, df_year, df_top_crimes = read_all_tables_from_postgres(table_id)
    five_records = df_crimes.sort_values(by='occurred_date', ascending=False).head()
    f = os.listdir("./data/")
    labels = [f"Detailed table {table_id}", f"Table {table_id}_geo", f"Table {table_id}_crimes_by_hour",f"Table {table_id}_crimes_by_year", f"Table {table_id}_top_crimes", "5 latest records"]
    if len(f) == 0:
        del_labels = ["None", "SQL tables (all)"]
    else:
        del_labels = ["None", f"File {f[0]}", "SQL tables (all)"]
        filename = f

    with col1:
        st.write(f'See {dataset_id}.json in its working folder')
        with st.container(border=True):
            st.write("📂 *data* folder:")
            st.write(filename)

        options_dict_dfs = {'Detailed': df_crimes, '_geo': df_geo, '_hour': df_hour, '_year': df_year, '_top': df_top_crimes, '5 records': five_records}
        option2 = st.selectbox("Choose a table:", labels)
        for key in options_dict_dfs.keys():
            if key in option2:
                st.write(key)
                st.dataframe(options_dict_dfs[key])
    
    with col2:
        with st.container():
            option1 = st.selectbox('Choose something to delete:', del_labels)            
            if st.button ("Delete"):
                if os.path.exists(destination_path) and option1 == f"File {f[0]}":
                    os.remove (destination_path)
                elif option1 == 'None':
                    st.write('Nothing to delete')
                else:
                    tablenames = [f"{table_id}", f"{table_id}_geo", f"{table_id}_crimes_by_hour",f"{table_id}_crimes_by_year", f"{table_id}_top_crimes"]
                    for name in tablenames:
                        sqlquery = f'DROP TABLE IF EXISTS {name};'
                        with engine.connect() as conn:
                            conn.execute(text(sqlquery))
                            conn.commit()
                            finished_workflow = 'false'

elif finished_workflow == 'false':
    st.info ("There's no data to be seen. Run 'Start Pipeline' button first, in 'See The Pipeline'")
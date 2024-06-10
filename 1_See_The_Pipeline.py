import streamlit as st
import time
import os
import subprocess
from dotenv import load_dotenv
import sys


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

# Building app
st.title("ETL Pipeline - Austin Crime Database 👮‍♂️")

col1, col2 = st.columns([5,4])

# App explanation in main page
with col1:
    with st.expander ("How to use this app:"):
        st.markdown (
        """  **SEE THE PIPELINE** :  \n  - Press **'See How Pipeline Works'** to know what the pipeline 
        does \n  - Press **'Start Pipeline'** to run the pipeline """)

    st.markdown(
        """This project simulates a small **ETL automated data pipeline**, using *Prefect*. ETL stands for 
        Extract-Transform-Load and in this app you can see how data goes through those steps:""")

    st.markdown(""" - Download data from https://data.austintexas.gov/ API as json file - Extract  \n  - Creation 
                of postgreSQL database to feed it with the .json file's info - Extract  \n  - Creation of relevant
                dataframes from the original table, a small data warehouse - Transform  \n  - Load dataframes 
                into the PostgreSQL database - Load  \n  - Creation of a dashboard (we want to see the data, right?)""")

    # Add buttons
    show_workflow_button = st.button ("See How Pipeline Works", type="primary")
    start_workflow_button = st.button ("Start Pipeline")
    if start_workflow_button:
        # See the log file once you start the workflow
        st.write ("Check log file from pipeline run")
        log1 = st.expander("Check log file for pipeline run")
            # Run the script file
            #result = subprocess.Popen(['bash', 'run.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #stdout, stderr = result.communicate()
            ## Display the terminal output
            #st.write('\n'.join(stdout.decode().split('\n')[1:][:-1]))


    # Shows how pipeline works
    with col2:
        container1 = st.container()
        if show_workflow_button:
            container1.empty()
            with container1:
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
                st.markdown(card((64, 153, 255),(255,255,255), "LOAD: PostgreSQL", "Load final tables into DB: data warehouse"), unsafe_allow_html=True)
                with st.spinner ('Loading new tables into PostgreSQL DB...'):
                    time.sleep(3)
                st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
                time.sleep(0.5)
                st.markdown(card((46, 216, 182),(255,255,255), "Visualize: Dashboard", "Create visuals with new tables"), unsafe_allow_html=True)
        
        # Real pipeline running
        if start_workflow_button:
            container1.empty()
            with container1:
                st.subheader("Running The Pipeline")
                with st.status ("Running Pipeline...", expanded=True, state='running') as status:
                    # Prepare lists for real time pipeline
                    msg = ["Download json file from API", "Create database and load .json into a table", "Transform data: clean, format, apply formulas", "Load final tables into DB and create data warehouse"]
                    small_msg = ["EXTRACT: a .json file", "EXTRACT: to PostgreSQL database", "TRANSFORM: Pandas/Numpy", "LOAD: PostgreSQL"]
                    card_colors = [(46, 216, 182), (255, 182, 77), (255, 83, 112), (64, 153, 255)]
                    st.markdown(card((64, 153, 255),(255,255,255), "", "Pipeline Started"), unsafe_allow_html=True)
                    st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
                    # Start actual pipeline workflow with stdout showing in expander 'log1'
                    command = [f"{sys.executable}", '-u', 'etl-workflow.py']
                    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
                    i = 0 # Index to control which flow step to show
                    flowstep = False # flag to check if a flow step has been printed (and not reapeat it)
                    while process.poll() is None:
                        # Reading the stdout line of text
                        line = process.stdout.readline()
                        flowstep = False
                        # Showing pretty cards with flow steps
                        if "json file downloaded successfully" in line:
                            i = 0
                            flowstep = True
                        elif "created in postgreSQL" in line:
                            i = 1
                            flowstep = True
                        elif "loaded successfully from postgresql" in line:
                            i = 2
                            flowstep = True
                        elif "Finished creating final transformed tables" in line:
                            i = 3
                            flowstep = True
                        elif  "'All states completed.'" in line:
                            i = 4
                            flowstep = True
                        # continue with if's, if sql in line i = 1 ......
                        if i != 4 and flowstep:
                            st.markdown (card(card_colors[i], (255,255,255), msg[i], small_msg[i]), unsafe_allow_html=True)
                            st.markdown("<div style='text-align: center; font-size: 30px;'>🔽</div>", unsafe_allow_html=True)
                            time.sleep(0.5)
                        # writing the line in the app
                        if not line:
                            continue
                        with log1:
                            log1.write(line.strip())
                    # end of pipeline workflow execution
                    st.markdown(card((46, 216, 182),(255,255,255), "", "Pipeline Finished!"), unsafe_allow_html=True)
                    status.update(label="Pipeline finished!", state="complete", expanded=True)
                    os.environ["finished_workflow"] = 'true'


import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt


st.title('ETL Pipeline - Austin Crime DB')

st.markdown ("""This project simulates a small automated data pipeline:
             
 * it downloads data from https://data.austintexas.gov/ API as json file
 * creates a postgreSQL database and feeds it with the info from the json file
 * creates relevant dataframes from the postgre database - a small data warehouse.
 * The pipeline is run by a prefect ETL workflow """)


# Setting the diagram for the data flow
figure = plt.figure(figsize = (11,4))

From = ['Big Query\nAustin Crime\ndatabase', 'CSV file','PostgreSQL\nDatabase', 'Dataframes\nData Warehouse', 'PostgreSQL\nDatabase']
To = ['CSV file','PostgreSQL\nDatabase', 'Dataframes\nData Warehouse', 'PostgreSQL\nDatabase', 'Dataframes\nData Warehouse']

df = pd.DataFrame({ 'from':From,
                   'to':To})
# Define Node Positions
pos = {'Austin Crime\ndatabase API':(1,1),
       'JSON file': (2,1),
       'PostgreSQL\nDatabase': (3,1),
       'Dataframes\nData Warehouse': (4,1)}

# Define Node Colors
NodeColors = {'Austin Crime\ndatabase API':[0,1,1],
        'JSON file':[1,.5,1],
        'PostgreSQL\nDatabase':[1,0,1],
        'Dataframes\nData Warehouse':[0,0,1]}

Labels = {}
i = 0
for a in From:
    Labels[a]=a
    i +=1
Labels[To[-1]]=To[-1]

# Build your graph. Note that we use the DiGraph function to create the graph! This adds arrows
G=nx.from_pandas_edgelist(df, 'from', 'to', create_using=nx.DiGraph() )

# Define the colormap and set nodes to squares
Squares = []
Colors_Squares = []
for i in G.nodes:
    Squares.append(i)
    Colors_Squares.append(NodeColors[i])

# By making a white node that is larger, I can make the arrow "start" beyond the node
nodes = nx.draw_networkx_nodes(G, pos, 
                       nodelist = Squares,
                       node_size=1.25e4,
                       node_shape='s',
                       node_color='white',
                       alpha=1)

nodes = nx.draw_networkx_nodes(G, pos, 
                       nodelist = Squares,
                       node_size=1e4,
                       node_shape='s',
                       node_color=Colors_Squares,
                       edgecolors='black',
                       alpha=0.5)


nx.draw_networkx_labels(G, pos, Labels, font_size=12)

# Again by making the node_size larer, I can have the arrows end before they actually hit the node
edges = nx.draw_networkx_edges(G, pos, node_size=1.2e4, arrowstyle='->', width=2, arrowsize=10)

plt.xlim(0.4,5)
plt.ylim(0,2)
plt.axis('off')

# Add sidebar buttons
pipeline_button = st.sidebar.button ("Start Pipeline", type="primary")
dataflow_button = st.sidebar.button ("Data Flow")
if pipeline_button:
    st.write ("Starting pipeline...")
    
if dataflow_button:
    st.header ('Data Flow')
    st.pyplot(figure)


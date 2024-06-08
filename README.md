# ETL PIPELINE - AUSTIN CRIME DATABASE 👮‍♂️

The aim of this small project is to showcase data engineering skills such as:

- Building data pipelines and data flows
- Extracting data
- Storing data
- Cleaning data (remove nan's, format data types, etc.)
- Tools used: SQL, Python (Pandas, SQLAchemy), Prefect, Streamlit

## ETL Pipeline App

This data engineering project simulates a small **ETL automated data pipeline**, using *Prefect*. [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) stands for Extract, Transform and Load, a common three-phase process to prepare data. In this app you can see how data goes through those steps:

- EXTRACT phase: Downloading data from https://data.austintexas.gov/ API as json file. This website consists in the 'City of Austin Open Data Portal' and contains databases related with several public reports related with various subjects such as environment, health, transportation, community services, etc. The database about Public Safety harbours a very well known databse used in several data projects: Austin Crime. And this is the one we will use to extract a .json file using the website API
 - EXTRACT phase: Creating a postgreSQL database to feed it with the .json file's info. SQL databases are very common and widespread, so it made sense to use it to keep our data warehouse.
 - TRANSFORM phase: Creating relevant SQL tables and corresponding python dataframes from the original table, to biuld our data warehouse. These tables will contain cleaned and correctly formatted data, as well as aggregations of the most relevant subjects (number of crimes per year, daily histogram, top crimes commited and crimes geographical distribution among the city of Austin). 
 - LOAD phase: Tables will be kept in the postgreSQL database.

The pipeline retrieves a .json file from https://data.austintexas.gov/ API and saves it into a local folder called *data*. The pipeline then creates 5 tables into our postgreSQL database: 
- a detailed table with cleaned data from the .json file (most relevant columns)
- 4 final tables with aggragated / resumed data for analysis. 

The app allows you to delete all the data collected to show we have an actual pipeline running in Prefect, and is not just showing some preloaded data and a fancy pipeline animation. 

Finally, the app builds a very simple dashboard with 5 visuals to show what you can build with the collected data. The aim of this project is not to analyse it, but it is interesting to observe, for example, that 2024 has around 800 crimes reported, while for other years the number is very low, and some years are missing. We may have biased data here, where what we are actually seeing is not a real yearly distribution of crimes, but a poor data collection (or perhaps the database was cleaned in 2024, from previous years)

## How To Use This App

The app is divided in 3 pages: 'See The Pipeline', 'See the Data' and 'See The Dashboard'

**SEE THE PIPELINE** :
- Press **'See How Pipeline Works'** to know what the pipeline does. This is a demo animation of what the pipeline is going to do. 
- Press **'Start Pipeline'** to run the pipeline. See the pipeline running in real time, by checking the various pipeline steps along with log messages from Prefect logger.

**SEE THE DATA** :
- See the downloaded .json file in the local folder *data*. You can delete it and see a new file after the pipeline upload it again
- See the latest 5 records of the 'crime' table . Check the dates there (date of occurence or clearance date) and see how close they are from your current date.
- Choose a table from the dropdown list to browse and visualize data from the SQL tables. You can delete the tables and see the tables showing up again after rerunning the pipeline

**SEE THE DASHBOARD** :
- This dashboard shows 2 bar charts, 2 pie charts and one map with most interesting information about crime in Austin. The visuals are simple and no analysis is performed. This is simply to show how we can do with the data once the data warehouse is ready.

## Project Challenges And Conclusions

The most difficult part of this project was to deploy it with a postgreSQL database in the Streamlit server.

Using API's and SQL databases is very common in data engineering and so they were chosen to build this data pipeline. However, this would not be the best choice to store information with the app live in the Streamlit server. I would rather have used Big Query from Google to store the data, due to connection and data handling simplicity.  

Most data manipulation and data cleaning was done using Pandas package. However, we could have easily adapted this project to use SQL queries (if they were pre-built).
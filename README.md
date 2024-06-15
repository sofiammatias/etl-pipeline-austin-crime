# ETL PIPELINE - AUSTIN CRIME DATABASE 👮‍♂️

The aim of this small project is to showcase data engineering skills such as:

- Creating a database from existing containers
- Building data pipelines and data flows
- Extracting data
- Storing data
- Cleaning data (remove nan's, format data types, etc.)
- Tools used: SQL, Python (Pandas, SQLAchemy), Prefect, Streamlit, Docker, Google Compute Engine

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

Using API's and SQL databases is very common in data engineering and so they were chosen to build this data pipeline. The most difficult part of this project was to deploy a postgreSQL database to work with the app running in the Streamlit Community server. It could not be a local solution, as the Streamlit server can't access my local machine. Hosting Postgres databases in a platform was overkill and overpriced everywhere (including Google Cloud SQL). 

In the end, I've ended up using the most economic solution I could find: a **virtual machine** with PostgreSQL installed that runs in [Free Tier Google Compute Engine](https://cloud.google.com/free/docs/free-cloud-features#compute) tool. The virtual machine consists in a **e2-micro machine type** with 2 shared vCPUs, 1Gb of Memory and 10Gb of disk allocated. See how to deploy a Postgres database in a cheap virtual machine in [this post](https://joncloudgeek.com/blog/deploy-postgres-container-to-compute-engine/). 

The virtual machine is not very reliable, as it can be stopped at any time due to the virtual machine settings (shared hardware allocation with minimum priorities). However, this app will be used as a demo, probably just a couple of hours in total per month, and only by a user at a time. There's no need for high-performance, 24/7 reliable solutions. It just needs to run when the app is activated. The minimum settings are enough for this application and the cost varies between 0$-3$.

For local development and testing, I've used a **PostgreSQL database running in a Docker container**. This solution allows to easily configure and run a Postgres database for other needs, using the compose.yaml file. Data manipulation and data cleaning was done using Pandas package. However, we could have easily adapted this project to use SQL queries (if they were pre-built).

As a footnote, and if we were to use 4 to 5 tables with less that 50 columns each in a real life case, the most suitable solution to store data would be to simply use a csv file inside the app. SQL databases would be overkill for this, especially because data is stored elsewhere and you only wish to access it.


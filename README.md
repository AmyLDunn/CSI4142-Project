# CSI4142-Project

Compilation instructions

The code can be found in our gitHub repository here: AmyLDunn/CSI4142-Project (github.com)

To run this code, download the latest release of version 13 of Postgresql: PostgreSQL: Downloads

Once installed, launch pgAdmin and create a database, be sure to mark down the database_name, the port number and the database password used as all will be used while connecting to the database via python code. [we recommend using the default port, database name =fire_hazard  and a password of CSI4142 to keep everything consistent with our code meaning that you wouldn’t need to change anything]

In pgAdmin, open the query tool and paste the sql code found in sql_code_to_run.txt and run it to create the needed tables.

While all the tables will be created, they will not be populated. To populate the tables, go into the code section of our GitHub repository and run the following pieces of code in the following order:

Demographics_data_processing_and_table_creation.ipynb

FireZone.ipynb

loading_dim_location.ipynb

Loading_dim_date.py 

loading_dim_weather.ipynb

loading_fact_table.py

N.B. You may need to pip install some packages to run this code depending on what you have already installed onto your computer

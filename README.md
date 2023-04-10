# CSI4142-Project

## Compilation instructions

The code can be found in our gitHub repository here: AmyLDunn/CSI4142-Project (github.com)

To run this code, download the latest release of version 13 of Postgresql: PostgreSQL: Downloads

Once installed, launch pgAdmin and create a database, be sure to mark down the database_name, the port number and the database password used as all will be used while connecting to the database via python code. [we recommend using the default port, database name =fire_hazard  and a password of CSI4142 to keep everything consistent with our code meaning that you wouldn’t need to change anything] If you do use a different naming convention be sure to change line 239 in create_database.py.

In pgAdmin, open the query tool and paste the sql code found in sql_code_to_run.txt and run it to create the needed tables.

While all the tables will be created, they will not be populated. To populate the tables, go into the code section of our GitHub repository and run the create_database.py file.

N.B. You may need to pip install some packages to run this code depending on what you have already installed onto your computer. They are commented out in lines 2-11 of the create_database.py file.

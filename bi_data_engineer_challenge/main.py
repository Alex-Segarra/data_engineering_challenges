from processes import extract_and_clean_data
from processes import import_and_create
from utils import setup

#Setting up database and connection for the first time
conn = setup.setup_db(install_pg=True)

#Downloading data from the web
extract_and_clean_data.extract_data(conn)

#Cleaning imported data
extract_and_clean_data.clean_data()

#Bringing data into the data_mart database
import_and_create.create_new_database()

#Creating tables
import_and_create.create_new_tables()
# BI Data Engineer Challenge

This was part of a challenge issued to me to see if I could run an ETL.  The stated technologies included python and postgresql - which is what I used.  Lacking the ability to use any sophisticated technologies, I stuck as closely to base level libraries found in any python/Linux distributions (with the exception of psycopg2).

Please refer to the documents in the "requirements" folder for more information about the specifications.

### Requirements
- Linux Distribution (Ubuntu or Debian)
- Python 3.7 or above
-- psycopg2 library
- Jupyter Notebook (recommended)

### This folder contains:

- `process` folder: contains the code that processes the data
- `sql` folder: contains the SQL code that creates the `data mart`
- `uninstall` folder: contains the code that uninstalls postgres, deletes the databases and users, and removes the creds.json file as well as the `data` folder.
- `utils` folder: contains the code that contains setup, init, and helper functions.
- `config.json` file: contains the configuration for the challenge.  **Please be sure to update the config.json file as needed.**  In particular, be sure to modify the `home_folder` value to reflect the location of the project folder.
- `main.py` file: contains the code that runs the challenge.  **Run this file to start the whole process.**

### In addition:
- `functions_test.ipynb` file: a notebook that contains tests to ensure that the process worked.  Currently it displays its successful results.
- `reports_test.ipynb` file: a notebook that contains tests to ensure that the project requirements were met.

*Critically, these files can be re-run after running the `main.py` file to ensure that the process worked.*

### After Running `main.py`, the following files/folders will be created:
- `data` folder: contains the raw data downloaded from the web
- `creds.json` file: contains autmatically generated credentials for the challenge

### To clean up your workspace, run the following commands from the uninstall folder:
- `bash uninstall.sh`
This will uninstall postgres, delete the databases and users, and remove the `creds.json` file as well as the `data` folder.

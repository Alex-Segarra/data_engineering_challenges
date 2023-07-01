import os
import requests
import csv
import re
import datetime
import subprocess
try:
  import psycopg2
except ImportError as e:
  print(e)
  subprocess.getoutput('pip install psycopg2-binary')
  import psycopg2
from utils.tools import init_db


def pull_data(url,path):
    
    """
    PURPOSE: Pull data from a url.
    
    INPUT:
        url: the url of the data
        path: the path to save the data
        
    OUTPUT:
        A CSV file location
    """
    
    response = requests.get(url)
    
    if not os.path.exists(path):
        os.makedirs(path)
        
    if not os.path.exists(os.path.join(path,'data')):
        os.makedirs(os.path.join(path,'data'))
    
    fpath=os.path.join(path,'data')
    
    file_name = os.path.join(fpath,"data.csv")
    
    with open(file_name, 'wb') as f:
        f.write(response.content)
        
    return file_name

def detect_data_type(filepath):
    
    """
    PURPOSE: Detect the data type of a row.
    
    INPUT:
        filepath: the file to be analyzed
        
    OUTPUT:
        A string indicating the data type of the row
    """
    
    with open(filepath, 'r') as f:
        csvreader = csv.reader(f)
        header = next(csvreader)
        second_row = next(csvreader)
    
    table_schema = []
    
    for i,x in enumerate(second_row):
        try:
            int(x)
            table_schema.append(f'{header[i]} int8')
            continue
        except ValueError:
            pass
        
        try:
            float(x)
            table_schema.append(f'{header[i]} float8')
            continue
        except ValueError:
            pass
        
        try:
            datetime.datetime.strptime(x,"%m/%d/%y")
            table_schema.append(f'{header[i]} date')
            continue
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(x,"%Y/%m/%d")
            table_schema.append(f'{header[i]} date')
            continue
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(x,"%Y-%m-%d")
            table_schema.append(f'{header[i]} date')
            continue
        except ValueError:
            pass

        try:
            str(x)
            table_schema.append(f'{header[i]} text')
            continue
        except ValueError:
            pass
        
    return ',\n'.join(table_schema)

def load_table_from_csv(filepath,table,conn,first_row=False):
    
    """
    PURPOSE: Load a table from a CSV file.
    
    INPUT:
        filepath: the path to the CSV file
        database: the name of the database
        table: the name of the table.  Must include schema.
        first_row: If "FALSE" will automatically strip the first row of the CSV file. Otherwise, it requires a string of the first row headers.
        conn: the connection to the database
        
    OUTPUT:
        None
    """
    
    cursor = conn.cursor()
    
    #parses the first row
    if not first_row:
        with open(filepath, 'r') as f:
            first_row = f.readline().strip()#.split(',')
            table_schema = detect_data_type(filepath).strip()
    else:
        if type(first_row) == str:
            print('First row is a string. Please be sure it is a table schema')
        else:
            raise Exception("First row must be a string list of header/column names")
    
    #Check to see if table name conforms
    if not re.match(r'^.*[.].*', table):
        raise Exception(f"Table name must include schema.  For example: 'schema.table.' Its currently: {table}")
    else:
        schema = re.match(r'(.*)\.(.*)',table).group(1)
        print(f'Looking for schema: {schema}')
        try:
            sql = f"""CREATE SCHEMA {schema};"""
            cursor.execute(sql)
        except psycopg2.errors.DuplicateSchema:
            print(f"Schema {schema} already exists.  Continuing...")
            pass
    
    #Clear out table
    sql = f"""DROP TABLE IF EXISTS {table};"""
    cursor.execute(sql)
    
    #Check to see if the table already exists
    
    try:
        print(f"Copying table {table} from {filepath}...")
        sql = f"""copy {table} ({first_row}) FROM '{filepath}' DELIMITER ',' CSV HEADER;"""
        cursor.execute(sql)
        print(f"Table {table} loaded.")
    except psycopg2.errors.UndefinedTable as e:
        print(e)
        sql = f"""CREATE TABLE {table} ({table_schema});"""
        cursor.execute(sql)
        print(f"Table {table} created.")     
        print(f"Now... copying table {table} from {filepath}...")
        sql = f"""copy {table} ({first_row}) FROM '{filepath}' DELIMITER ',' CSV HEADER;"""
        cursor.execute(sql)
        print(f"Table {table} loaded.")
        
def load_data(dbname,url,table,path,conn):

    """
    Purpose: Load data from csv file
    Inputs: 
        dbname - database name
        url - url of csv file
        table - table name
        path - path of home directory
    Outputs: None
    """

    #Connecting to database
    #conn = setup.setup_db()
    
    #Creating database
    init_db.create_db(dbname,conn)
    conn = init_db.connect_to_db(dbname)

    #Loading data
    fpath = pull_data(url,path)
    print('Data Downloaded')
    
    #Putting data in database
    load_table_from_csv(filepath = fpath,table = table,conn=conn,first_row=False)
    
    print("Data loaded")
import os
import re
import json
import subprocess
try:
  import psycopg2
except ImportError as e:
  print(e)
  subprocess.getoutput('pip install psycopg2-binary')
  import psycopg2
from utils.tools import init_db
from utils.tools import load_operations
from utils.tools import query_operations
from utils import setup

def extract_data(conn):
    file = json.load(open('./config.json'))
    init_db.create_db(file['raw_db_name'],conn)
    conn = init_db.connect_to_db(file['raw_db_name'])
    load_operations.load_data(dbname=file['raw_db_name'], url=file['url'], table=file['import_table_name'], path=file['home_folder'],conn=conn)   

def clean_data():
    file = json.load(open('./config.json'))
    
    conn = init_db.connect_to_db(file['raw_db_name'])
    cur = conn.cursor()
    
    schema = re.match(r'(.*)\.(.*)',file['import_table_name']).group(1)
    table = re.match(r'(.*)\.(.*)',file['import_table_name']).group(2)
    
    #Running Deduplication first
        
    output= ["Creating Dedupe Schema..."
            ,"Creating dedupe table..."
            ,"Dropping old table..."
            ,"Renaming new table..."
            ,"Renaming new table with new schema..."
            , "Dropping cleaning schema to clean up..."]
    
    command = [f"CREATE SCHEMA cleaning;"
             ,f"CREATE TABLE cleaning.dedupe AS SELECT DISTINCT * FROM {file['import_table_name']};"
             ,f"DROP TABLE {file['import_table_name']};"
             ,f"ALTER TABLE cleaning.dedupe RENAME TO {table};"
             ,f"ALTER TABLE cleaning.{table} SET SCHEMA {schema};"
              ,f"DROP SCHEMA cleaning"]
    
    for k,v in dict(zip(output,command)).items():
    
        print(k)
        sql = v
        query_operations.run_query_safe(sql,conn)
        
    # Trimming Whitespace
    col_names = query_operations.get_column_names(table,schema,conn,condition="AND data_type='text'")
    print(f"{file['import_table_name']} has been deduped.")
    
    sql = f"UPDATE {schema}.{table}\n"
    for i,c in enumerate(col_names):
        
        if i == 0:
            sql += f"SET {c} = TRIM({c}),\n"
        elif i == len(col_names)-1:
            sql += f"{c} = TRIM({c})\n"
        else:
            sql += f"{c} = TRIM({c}),\n"
    sql += ";"
    
    query_operations.run_query_safe(sql,conn)
    print(f"Whitespace in{file['import_table_name']} has been removed.")
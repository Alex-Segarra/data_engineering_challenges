#!/usr/bin/env python3
import subprocess
import os
import json
try:
  import psycopg2
except ImportError as e:
  print(e)
  subprocess.getoutput('pip install psycopg2-binary')
  import psycopg2

def install_postgres():
    
    """
  PURPOSE: install postgres
  
  INPUT:
    None
    
  OUTPUT:
    None
    """
    
    subprocess.getoutput("sudo apt update")
    #print(output)
    subprocess.getoutput("sudo apt-get -y install postgresql-contrib")
    #print(output)
    
def init_db(dbname):
    """
  PURPOSE: create a database from scratch.
  
  INPUT:
    dbname: the name of the database
    
  OUTPUT:
    None
    
  """
    print(f"Creating database {dbname}...")
    subprocess.getoutput(f'''sudo -u postgres psql -c "CREATE DATABASE {dbname};"''')
    print(f"Database {dbname} created.")
  
    
def create_db(dbname,conn):
    
    """
    PURPOSE: Create a database.
    
    INPUT:
        dbname: the name of the database
        conn: the connection to the database
        
    OUTPUT:
        None
    """
    
    try:
        cur = conn.cursor()

        sql = f'''CREATE database {dbname};''';

        #Creating a database
        cur.execute(sql)
        print(f"Database created {dbname}........")
        
    except psycopg2.errors.DuplicateDatabase as e:
        print(e)

def init_user(username, password):
    
    """
  PURPOSE: Assign a user/password to the database
  
  INPUT:
    username: the username of the user
    password: the password of the user
    dbname: the name of the database
    
  OUTPUT:
    None
    """
    print(f"Creating user...{username}")
    subprocess.getoutput(f"""sudo -u postgres createuser {username} -p 5432 --no-password -s""")

    print(f"Giving {username} a password...")
    subprocess.getoutput(f'''sudo -u postgres psql -c "ALTER USER {username} WITH PASSWORD '{password}';"''')

    print(f"Giving {username} CREATEDB permissions...")
    subprocess.getoutput(f'''sudo -u postgres psql -c "ALTER USER {username} CREATEDB;"''')

    print(f"Making {username} a superuser...")
    subprocess.getoutput(f'''sudo -u postgres psql -c "ALTER USER {username} WITH SUPERUSER;"''')
                         
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password
    
    data = {"username":username, "password":password}
    
    try:
        os.remove("creds.json")
    except:
        print("no creds.json file")
    
    with open("creds.json", "a") as f:
        json.dump(data, f)
    
    
def remove_user(username):
    
    """
  PURPOSE: remove a user
  
  INPUT:
    username: the username of the user
    
  OUTPUT:
    None
    """
    print(f"Removing {username}...")
    subprocess.getoutput(f'''sudo -u postgres psql -c "DROP USER {username}"''')
                         
    del os.environ["PGUSER"]
    del os.environ["PGPASSWORD"]
    
    os.remove("creds.json")
    
def connect_to_db(dbname):
    
    """
  PURPOSE: connect to the database by calling out to a shell script. Used if the database does not exist.
  
  INPUT:
    username: the username of the user
    password: the password of the user
    
  OUTPUT:
    conn: the connection to the database
    """
    print("Connecting to database...")

    #Connect and create cursor
    try:
        print(f'Logging in as {os.environ["PGUSER"]}')
        conn = psycopg2.connect(database=dbname, user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"], host='127.0.0.1', port= '5432')
    except KeyError:
        file = json.load(open('creds.json'))
        conn = psycopg2.connect(database=dbname, user=file['username'], password=file['password'], host='127.0.0.1', port= '5432')

    conn.autocommit = True
    
    return conn

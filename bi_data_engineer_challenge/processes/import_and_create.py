from utils.tools import init_db
from utils.tools import query_operations

def create_new_database():
    
    conn = init_db.connect_to_db('raw_data')
    print("Creating new database...")
    query_operations.run_query_safe('CREATE DATABASE data_mart;', conn)
           
def create_new_tables():
    
    conn = init_db.connect_to_db('data_mart')
    cur = conn.cursor()
    
    print("Creating Star Schema for new tables...")

    with open("./sql/import_and_create.sql", 'r') as f:
        sql = f.read()
        print(sql)

    sql_list = sql.split(";")

    for s in sql_list:
        s += ";"
        query_operations.run_query_safe(s, conn)
import subprocess
try:
  import psycopg2
except ImportError as e:
  print(e)
  subprocess.getoutput('pip install psycopg2-binary')
  import psycopg2

def run_query_safe(sql,conn):
    
    """
    PURPOSE: safe version of run_query that catches errors fixed by rollback
    
    INPUT:
        sql: string
        conn: psycopg2 connection
    OUTPUT:
        None
    """
    
    cur = conn.cursor()
   
    try:
        cur.execute(sql)
        conn.commit()
    except psycopg2.Error as errorMsg:
        print(errorMsg)        
        conn.rollback()
        
def get_column_names(table,schema, conn, condition=False):
    """
    Purpose: extracts columns names
    
    Input: 
        table - string: name of the table
        schema - string: name of the schema
        conn - psycopg2 connection
        condition - string: A SQL condition for the where clause
        
    Output: 
        list of columns names
    """
    
    cur = conn.cursor()
    
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{schema}'
    AND table_name = '{table}'"""
    
    if condition:
        sql += condition
        
    sql += ";"
    
    cur.execute(sql)
    
    return [i[0] for i in cur.fetchall()]
 
    
    
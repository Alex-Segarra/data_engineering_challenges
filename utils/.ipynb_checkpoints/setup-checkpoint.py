from utils.tools import init_db
from utils.tools import load_operations

def setup_db(install_pg=False):
    """
    Purpose: Setup database
    
    Inputs: None
    
    Outputs: 
        conn - database connection
    """

    # will install postgresql if needed
    if install_pg:
        print('Installing postgresql...')
        init_db.install_postgres()
        
    # Creates an admin user
    init_db.init_user('admin', 'password')
    
    # Creates a local db to launch
    init_db.init_db('localhost')
    
    # Creates a connection to local db
    conn = init_db.connect_to_db('localhost')
    
    # returns connection
    return conn
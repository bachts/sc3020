'Connecting postgreSQL to Python'
import psycopg2
from config import config
from psycopg2 import sql
import sys
import re

import explore
import interface



def remove_linebreaks_and_extra_spaces(input_string):
    # Remove line breaks
    without_linebreaks = input_string.replace('\n', ' ').replace('\r', '')

    # Replace multiple spaces with a single space
    without_extra_spaces = re.sub(r'\s+', ' ', without_linebreaks)

    # Strip leading and trailing spaces
    result = without_extra_spaces.strip()

    return result

def get_unique_tuples(rows):
    tuple_locations = set(list())
    for (data,) in rows:
        values = list(map(int,[value.strip('"()"') for value in data.strip("'{}'").split(',')]))
        for i in range(len(values)//2):
            tuple_locations.add(tuple([values[2*i],values[2*i+1]]))
            
    print(tuple_locations)
        
    
    


def query_input(crsr):
    print("Input your SQL Query: ")
    query = sys.stdin.read()
    
    query= remove_linebreaks_and_extra_spaces(query) #Clean the query
    
    modified_query = explore.ctid_query(query) #Get the blocks accessed
    modified_query = sql.SQL(modified_query)  
    crsr.execute(modified_query)
    
    
    
    
    rows = crsr.fetchall()
    for row in rows:
        print(row)
    get_unique_tuples(rows)
    
    
    
    
    # query = sql.SQL("SELECT * FROM pg_catalog.pg_tables WHERE schemaname!='pg_catalog' AND schemaname!='information_schema'")  
    # crsr.execute(query)
    # rows = crsr.fetchall()
    # for row in rows:
    #     print(row)
        
    # query = sql.SQL("explain (analyze, buffers, costs off)  SELECT * FROM nation WHERE n_name='CHINA'")  
    # crsr.execute(query)
    # rows = crsr.fetchall()
    # for row in rows:
    #     print(row)
        
            
    

def connect():
    connection = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params)

        # create a cursor
        crsr = connection.cursor()
        print('PostgreSQL database version: ')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)
        
        print('PostgreSQL database connected')
        
        # crsr.execute('show block_size')
        # block_size = crsr.fetchone()
        #print(block_size)
        print('#############################################################################')
        query_input(crsr)
        crsr.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')
            
if __name__ == "__main__":
    # connect()
    interface.starting_menu()



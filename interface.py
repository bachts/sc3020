import psycopg2
from psycopg2 import sql

def display_blocks(locations,crsr):
    for table in locations.keys():
        
        query = sql.SQL(f'SELECT ctid,* FROM {table} ORDER BY ctid')
        crsr.execute(query)
        rows = crsr.fetchall()
        print(rows)
        for row in rows:
            
        
        
        
    
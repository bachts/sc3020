import psycopg2
from psycopg2 import sql

import explore

def display_blocks(locations,query,crsr):
    print("Displaying blocks")
    relation_details={}
    
    relations = explore.extract_original_tables(query)
    for relation in relations:        
        #OrderedDict(relation:[block][tuple_details])
        #[ [block1],[block2],...]
        
        query = sql.SQL(f'SELECT ctid,* FROM {relation} ORDER BY ctid')
        crsr.execute(query)
        rows = crsr.fetchall()
        
        block_content={}
        
        for row in rows:
            block,offset = list(map(int,row[0].strip('"()"').split(',')))
            tuple = [offset] + list(row[1:]) 
            if block not in block_content.keys():
                block_content[block]=[tuple]
            else:
                block_content[block].append(tuple)
        
            relation_details[relation]=block_content
    for relation,content in relation_details.items():
        for block,tuples in content.items():
            print(f'BLOCK {block}')
            for tuple in tuples: 
                print (tuple)
            print("#####################################################################################################################################################################")
            
            
        
        
    
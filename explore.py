import psycopg2
from psycopg2 import sql
import json
from sql_metadata import Parser
from config import config
import re
from collections import OrderedDict

def get_database_tables(cursor):
  ''' Return all tables in the schema and their column names and data types'''
  result = {}
  tables = []
  query = '''SELECT tablename FROM pg_catalog.pg_tables 
                 WHERE schemaname!='pg_catalog' AND
                 schemaname!='information_schema' '''
  cursor.execute(query)
  rows = cursor.fetchall()
  print(type(rows))
  for row in rows:
    tables.append(row[0])
  
  for table in tables:
    columns = []
    query = f'''SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{table}' '''
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
      columns.append((row[0], row[1]))
    result[table] = columns
    
  return result

def extract_table_names(query):
  
  ''' Return all relations used in a SQL query statement
      Input: A SQL Query
      Returns: A list of table names'''
  parser = Parser(query)
  query = parser.generalize
  parser = Parser(query)
    
  relations = parser.tables
  # print(relations)
  aliases = parser.tables_aliases
  # print(aliases)
  sql_keywords = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'JOIN',
    'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'OUTER JOIN', 'ON', 'AS',
    'DISTINCT', 'UNION', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
    'DELETE', 'CREATE', 'TABLE', 'ALTER', 'ADD', 'DROP', 'INDEX',
    'CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'REFERENCES', 'CASCADE',
    'TRUNCATE', 'COMMIT', 'ROLLBACK', 'BEGIN', 'END', 'IF', 'ELSE',
    'CASE', 'WHEN', 'THEN', 'EXISTS', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 
    'LEFT INNER JOIN', 'RIGHT INNER JOIN',
    ]
  for k, v in zip(aliases.keys(), aliases.values()):
    
    for i in range(len(relations)):
      if relations[i]==v and k.upper() not in sql_keywords:
        relations[i] = k
        break
  
  return relations

def extract_original_tables(query):
  
  parser = Parser(query)
  try:
    relations=parser.tables
    return relations
  except:
    return False




def remove_linebreaks_and_extra_spaces(input_string):
    # Remove line breaks
    without_linebreaks = input_string.replace('\n', ' ').replace('\r', '')

    # Replace multiple spaces with a single space
    without_extra_spaces = re.sub(r'\s+', ' ', without_linebreaks)

    # Strip leading and trailing spaces
    result = without_extra_spaces.strip()

    return result


def get_unique_tuples(rows,relations):
    print("Collecting tuples")
    import ast

    tuple_locations = OrderedDict((relation, []) for relation in relations)

    print(rows)
    for row in rows:
        for i, relation in enumerate(relations):
            tuple_in_row = list(ast.literal_eval(row[i]))
            print(tuple_in_row)
            tuple_locations[relation].append(tuple_in_row)
            # values = list(map(int,[value.strip('"()"') for value in data.strip("'{}'").split(',')]))
        
    for relation in relations:
        print(relation)
        print( tuple_locations[relation])
    return tuple_locations
        
def connect():
  try:
    global connection
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
    
    return connection
    
  except(Exception, psycopg2.DatabaseError) as error:
    return error
    
              
def disconnect(connection):
  connection.close() 
  print('Closed connection')
  
def ctid_query(query):
  '''Extract block and position in block using ctid'''
  print('ctid_query')
  relations = extract_table_names(query)
  #ctid_list=[]
  
  modified_query_ctid="SELECT "
  if 'group by' in query.lower(): #If there is GROUP BY clause
      for i,relation in enumerate(relations):
          modified_query_ctid+=f'ARRAY_AGG({relation}.ctid) AS {relation}_ctid '
          #ctid_list.append(f'{relation}_ctid')
          if i+1<len(relations):
            modified_query_ctid+=', '
  else:
      for i,relation in enumerate(relations):
            modified_query_ctid +=f"{relation}.ctid "
            if i+1<len(relations):
              modified_query_ctid+=', '
  
  from_index = query.upper().find('FROM')
  modified_query_ctid+=query[from_index:]
  # print(modified_query_ctid)
  
  order_index = modified_query_ctid.upper().find('ORDER BY')
  if order_index!=-1:
    modified_query_ctid=modified_query_ctid[:order_index]
  
  # order_index =modified_query_ctid.upper().find('ORDER BY')
  # modified_query_ctid=modified_query_ctid[:order_index] 

  return modified_query_ctid, relations #, ctid_list

def explain_analyze(query):
  '''Add the necessary explain analyze to a SQL query'''
  return 'explain (analyze, buffers, costs, format json) ' + query


def qep_tree(cursor, query):
  '''Generate QEP tree using Postgres' EXPLAIN ANALYZE function'''
  '''Cursor: Connection cursor from psycopg2
     Query: SQL query statement
     
     Returns: QEP in the form of a JSON document'''
  explanation = explain_analyze(query)
  cursor.execute(explanation)
  all = cursor.fetchone()
  json_string = json.dumps(all[0][0], indent=2)

  # Extract tree edges from the dict
  with open('queryplan.json', 'w') as f:
    json.dump(all[0][0], f, indent=2)
    
  return json_string

def loadjson():
  with open('queryplan.json') as json_file:
        data = json.load(json_file)
  dict_plan_inner = data
  return dict_plan_inner
  
def process(cursor, query):
  
  '''Process a query and return the output, with block id and access'''
  
  # print(ctid_query(query)[0])
  # cursor.execute(ctid_query(query)[0])

  try:
    cursor.execute(ctid_query(query)[0])
    output = cursor.fetchall()
    pretty_plan = qep_tree(cursor, query)
    plan = loadjson()
    return output, plan
  except:
    cursor.execute('ROLLBACK')
    connection.commit()
    return False, False
    

def display_blocks(relations ,crsr):
  
  '''Return the blocks accessed by the query plan
     Input: '''
  
  print("Displaying blocks")
  relation_details={}
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
          # print(f'BLOCK {block}')
          for tuple in tuples: 
              # print (tuple)
              pass
  return relation_details

def get_parent(json_tree):
  pass
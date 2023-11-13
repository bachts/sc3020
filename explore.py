import psycopg2
import json
from sql_metadata import Parser

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
  
  relations = parser.tables
  aliases = parser.tables_aliases
  
  for k, v in zip(aliases.keys(), aliases.values()):
    
    for i in range(len(relations)):
      if relations[i]==v:
        relations[i] = k
        break;
  
  return relations
  
  
def ctid_query(query):
  '''Extract block and position in block using ctid'''
  selected_index = query.find('SELECT')
  relations = extract_table_names(query)
  modified_query="SELECT "
  
  print(relations)
  for relation in relations:
    modified_query+=f"{relation}.ctid,"
    print(modified_query)
  modified_query+=query[selected_index+len('SELECT')+1:]
  # print(modified_query)
  return modified_query

def explain_analyze(query):
  '''Add the necessary explain analyze to a SQL query'''
  return 'explain (analyze, buffers, format json) ' + query


def qep_tree(cursor, query):
  '''Generate QEP tree using Postgres' EXPLAIN ANALYZE function'''
  '''Cursor: Connection cursor from psycopg2
     Query: SQL query statement
     
     Returns: QEP in the form of a JSON document'''
  explanation = explain_analyze(query)
  cursor.execute(explanation)
  all = cursor.fetchone()
  json_string = json.dumps(all[0][0], indent=2)

  return json_string

def process(cursor, query):
  
  '''Process a query and return the output, with block id and access'''
  cursor.execute(ctid_query(query))
  output = cursor.fetchall()
  plan = qep_tree(cursor, query)
  
  return output, plan
  
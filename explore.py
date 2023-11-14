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
  query = parser.generalize
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
    'CASE', 'WHEN', 'THEN', 'EXISTS'
    ]
  for k, v in zip(aliases.keys(), aliases.values()):
    
    for i in range(len(relations)):
      if relations[i]==v and k not in sql_keywords:
        relations[i] = k
        break
  
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
  modified_query+=query[selected_index+len('SELECT')+2:]
  print(modified_query)
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
  
import psycopg2


def get_tables(cursor):
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
  
    ''' Return all relations used in a SQL query statement'''
    query = query.replace(";"," ")
    query = query.replace(","," ")
    import re
    relation_names = ['nation', 'customer', 'orders', 
                      'lineitem', 'part', 'supplier', 
                      'region', 'partsupp']
    print(query)
    
    sql_keywords = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'JOIN',
    'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'OUTER JOIN', 'ON', 'AS',
    'DISTINCT', 'UNION', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
    'DELETE', 'CREATE', 'TABLE', 'ALTER', 'ADD', 'DROP', 'INDEX',
    'CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'REFERENCES', 'CASCADE',
    'TRUNCATE', 'COMMIT', 'ROLLBACK', 'BEGIN', 'END', 'IF', 'ELSE',
    'CASE', 'WHEN', 'THEN', 'EXISTS'
    ]
    pattern = re.compile("^[a-zA-Z0-9_$]+$")
    relations = set()
    
    query_words = query.split()
    FROM_flag=False
    for i,word in enumerate(query_words):
        if(FROM_flag or word == 'FROM'):
            FROM_flag=True
            if (word in relation_names):
                if i+1<len(query_words) and bool(pattern.match(query_words[i+1]) and query_words[i+1] not in sql_keywords):
                    relations.add(query_words[i+1])
                else:
                    relations.add(word)
                    
    print(relations)
    return relations
  
def explain_analyze(query):
  query 


def qep_tree(query)
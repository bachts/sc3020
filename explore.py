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
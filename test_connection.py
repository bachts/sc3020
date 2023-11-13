import psycopg2


db_name = 'TPC-H'
user = 'postgres'
host = 'localhost'
password = 'postgres'
port=5432

conn = psycopg2.connect(database=db_name,
                        user=user,
                        host=host,
                        password=password,
                        port=port)

cur = conn.cursor()
def get_tables():
  ''' Return all tables in the schema and their column names and data types'''
  result = {}
  tables = []
  query = '''SELECT tablename FROM pg_catalog.pg_tables 
                 WHERE schemaname!='pg_catalog' AND
                 schemaname!='information_schema' '''
  cur.execute(query)
  rows = cur.fetchall()
  print(type(rows))
  for row in rows:
    tables.append(row[0])
  
  for table in tables:
    columns = []
    query = f'''SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{table}' '''
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
      columns.append((row[0], row[1]))
    result[table] = columns
    
  return result
# testing = input('Testing? [y/n]')

# if testing=='y':
 
#   query = '''SELECT ctid FROM nation LIMIT 5'''
#   cur.execute(query)
# else:
#   query = input('Your SQL query here: ')
#   cur.execute('explain ' + query)

# rows = cur.fetchall()
tables = get_tables()

conn.commit()
conn.close()

print(tables)
  
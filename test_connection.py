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
testing = input('Testing? [y/n]')

if testing=='y':
  query = '''SELECT * FROM pg_catalog.pg_tables 
               WHERE schemaname!='pg_catalog' AND
               schemaname!='information_schema' '''
  cur.execute(query)
else:
  query = input('Your SQL query here: ')
  cur.execute('explain ' + query)

rows = cur.fetchall()

conn.commit()
conn.close()

for row in rows:
  print(row)
  
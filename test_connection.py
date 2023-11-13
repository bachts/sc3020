import psycopg2
import explore

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
# testing = input('Testing? [y/n]')

# if testing=='y':
 
#   query = '''SELECT ctid FROM nation LIMIT 5'''
#   cur.execute(query)
# else:
#   query = input('Your SQL query here: ')
#   cur.execute('explain ' + query)

# rows = cur.fetchall()
tables = explore.get_tables(cur)

conn.commit()
conn.close()

print(tables)
  
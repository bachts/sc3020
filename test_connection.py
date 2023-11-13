import psycopg2
import explore
import json

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
for table in tables.keys():
  print(table)
  for col in tables[table]:
    print(f'\t{col[0]}')
    


# print(tables)
query = input('Enter your query here: ')
while query!='q':
  cur.execute('explain (analyze, buffers, format json) ' + query)
  all = cur.fetchone()
  print(all[0][0])
  json_string = json.dumps(all[0][0], indent=2)
  query = input()
  
print(json_string)
conn.commit()
conn.close()
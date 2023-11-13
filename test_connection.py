import psycopg2
import explore
import json
import sql_metadata

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
tables = explore.get_database_tables(cur)
for table in tables.keys():
  print(table)
  for col in tables[table]:
    print(f'\t{col[0]}')
    


# print(tables)

# print(explore.process(cur, query='select * from region, nation where r_regionkey = n_nationkey;'))
a, b = explore.process(cur, query='select * from region as RRR;')
print(a)
print(b)

conn.commit()
conn.close()
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
# tables = explore.get_database_tables(cur)
# for table in tables.keys():
#   print(table)
#   for col in tables[table]:
#     print(f'\t{col[0]}')
    


# print(tables)

# print(explore.process(cur, query='select * from region, nation where r_regionkey = n_nationkey;'))
a, b = explore.process(cur, query='''
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '93 day'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;''')
print(a)
print(b)



conn.commit()
conn.close()
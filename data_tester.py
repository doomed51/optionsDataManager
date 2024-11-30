import duckdb 
import pandas as pd 
import matplotlib.pyplot as plt

conn = duckdb.connect('data/options_data.db')

# # List tables in db 
# tables = conn.execute('SHOW TABLES').fetchdf()
# print(tables['name'])
# print('\n')

# ## Main options history table(s)
# read_query = 'SELECT * FROM options_historical_data'
# df = conn.execute(read_query).fetchdf()
# print('options_historical_data Table')
# print(df.columns)

# underlying table 
print('\n')
read_query = 'SELECT * FROM underlying_price_history'
df = conn.execute(read_query).fetchdf()
print(df)

## Collection Progress Table
print('\n')
# drop from collection_progress where batch_id = bfcc3d38b559a73d0e20eef98d69838d
delete_query = 'DELETE FROM collection_progress WHERE batch_id = \'a9220cbd8217ebd5ef232f29e026e10e\''
conn.execute(delete_query)
read_query = 'SELECT * FROM collection_progress'
df = conn.execute(read_query).fetchdf()
print(df[['batch_id', 'symbol', 'start_date', 'end_date', 'status']])


# # print num completed, started, in_progress
# print('\n')
# read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'COMPLETED\''
# df = conn.execute(read_query).fetchdf()
# print(f'completed: {df}')

# read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'STARTED\''
# df = conn.execute(read_query).fetchdf()
# print(f'started: {df}')

# read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'IN_PROGRESS\''
# df = conn.execute(read_query).fetchdf()
# print(f'in_progress: {df}')





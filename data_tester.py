import duckdb 
import pandas as pd 
import matplotlib.pyplot as plt

conn = duckdb.connect('data/options_data.db')

# get tables in db 
tables = conn.execute('SHOW TABLES').fetchdf()
print(tables)
print('\n')


read_query = 'SELECT * FROM options_historical_data'
df = conn.execute(read_query).fetchdf()
print(df)

# from options hist data select where strike = 602.0, expiry=2024-11-27, type=call
# read_query = 'SELECT * FROM options_historical_data WHERE strike = 602.0 AND expiry = \'2024-11-27\' AND "right" = \'C\''
# df = conn.execute(read_query).fetchdf()
# print(df)


print('\n')
# underlying table 
read_query = 'SELECT * FROM underlying_price_history'

# print SPY-587.0C-20241129
# read_query = 'SELECT * FROM underlying_price_history WHERE symbol = \'SPY\' AND expiry = \'2024-11-29\' AND strike = 587.0 AND "right" = \'C\''
# df = conn.execute(read_query).fetchdf()
# print(df)

print('\n')
read_query = 'SELECT * FROM collection_progress'
df = conn.execute(read_query).fetchdf()

print(df[['symbol', 'start_date', 'end_date', 'status']])

# print num completed, started, in_progress
print('\n')
read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'COMPLETED\''
df = conn.execute(read_query).fetchdf()
print(f'completed: {df}')

read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'STARTED\''
df = conn.execute(read_query).fetchdf()
print(f'started: {df}')

read_query = 'SELECT COUNT(*) FROM collection_progress WHERE status = \'IN_PROGRESS\''
df = conn.execute(read_query).fetchdf()
print(f'in_progress: {df}')





import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import pandas as pd

PASSWORD = os.getenv('SNOWSQL_PWD')
USER = os.getenv('USER')
ACCOUNT = os.getenv('ACCOUNT')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
SCHEMA = os.getenv('SCHEMA')

my_snowflake_data = {
    'user' : USER,
    'password' : PASSWORD,
    'account' : ACCOUNT,  
    'warehouse' : WAREHOUSE,
    'database' : DATABASE,
    'schema' : SCHEMA
}

# conn = snowflake.connector.connect(
#     user=USER,
#     password=PASSWORD,
#     account=ACCOUNT,
#     )

# cs = conn.cursor()
# try:
#     cs.execute('Select current_version()')
#     row = cs.fetchone()
#     print(row[])
# finally:
#     cs.close()
# conn.close()
def create_sf_stuff(my_snowflake_data):
    with snowflake.connector.connect(**my_snowflake_data) as conn:
        with conn.cursor() as cs:
            cs.execute('Select current_version()')
            row = cs.fetchone()  # method returns a single record or None if no more rows are available.
            print(row[0])
            print('creating warehouse...')
            cs.execute('CREATE WAREHOUSE IF NOT EXISTS piotr_o_warehouse')
            print('creating database...')
            cs.execute('CREATE DATABASE IF NOT EXISTS piotr_o_database')
            print('using database...')
            cs.execute('USE DATABASE piotr_o_database')
            print('creating schema...')
            cs.execute('CREATE SCHEMA IF NOT EXISTS piotr_o_schema')
            print('creation complete.')
            cs.execute('USE WAREHOUSE piotr_o_warehouse')
            cs.execute('USE DATABASE piotr_o_database')
            cs.execute('USE SCHEMA piotr_o_schema')
            print('creating a table...')
            cs.execute('''CREATE OR REPLACE TABLE piotr_comments (
                ID INTEGER, 
                comments STRING);''')
            print('insert a few rows...')
            cs.execute('''INSERT INTO piotr_comments (ID, comments)
                    VALUES (1, 'piotrs comment one')''')
            cs.execute('''INSERT INTO piotr_comments (ID, comments)
                    VALUES (2, 'piotrs comment two')''')
            print('read some rows...')
            cs.execute('SELECT * FROM piotr_comments')
            for row in cs.fetchall():
                print(row)
            print('complete')
            
def read_to_pandas_one(my_snowflake_data):
    with snowflake.connector.connect(**my_snowflake_data) as conn:
        with conn.cursor() as cs:
            cs.execute('Select current_version()')
            df = cs.fetch_pandas_all()
            return df

def read_to_pandas(my_snowflake_data):
    with snowflake.connector.connect(**my_snowflake_data) as conn:
        with conn.cursor() as cs:
            cs.execute('SELECT * FROM piotr_comments')
            df = cs.fetch_pandas_all()
            return df

def write_new_rows_frompd(data_dict, my_snowflake_data):
    newrows_df = pd.DataFrame.from_dict(data_dict)
    tab_name = 'piotr_comments'
    with snowflake.connector.connect(**my_snowflake_data) as conn:
        success, nchunks, nrows, _ = write_pandas(conn, newrows_df, tab_name, quote_identifiers=False)
    return newrows_df
            
def main():
    my_data_dict = {'ID' : [3, 4],
                    'COMMENTS' : ['piotrs comment three',
                                  'piotrs comment four']}
    write_new_rows_frompd(my_data_dict, my_snowflake_data)
    print(read_to_pandas(my_snowflake_data))
    
if __name__ == '__main__':
    main()
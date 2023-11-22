import snowflake.connector
import os

PASSWORD = os.getenv('SNOWSQL_PWD')
USER = os.getenv('USER')
ACCOUNT = os.getenv('ACCOUNT')

conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    )

cs = conn.cursor()
try:
    cs.execute('Select current_version()')
    row = cs.fetchone()
    print(row[0])
finally:
    cs.close()
conn.close()


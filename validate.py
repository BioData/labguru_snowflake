#!/usr/bin/env python
import snowflake.connector
import os

USER = os.getenv('SNOFLAKE_API_USER')
PASSWORD = os.getenv('SNOFLAKE_API_PASSWORD')
ACCOUNT = os.getenv('SNOFLAKE_API_ACCOUNT')

# Gets the version
ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

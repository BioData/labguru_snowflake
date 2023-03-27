#!/usr/bin/env python
import snowflake.connector
from snowflake.connector import connect

import os

SNOWFLAKE_USER = os.getenv('SNOFLAKE_API_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOFLAKE_API_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOFLAKE_API_ACCOUNT')
SNOWFLAKE_WAREHOUSE = 'lg_wharehouse'
SNOWFLAKE_DATABASE = 'lg_experiments'
SNOWFLAKE_SCHEMA = 'lg_experients_schema'
SNOWFLAKE_STAGE = 'lg_temp_stage'
SNOWFLAKE_TABLE = 'lg_experiments_table'

def upload_file_to_stage(ctx,stage_name, file_path):
  with ctx.cursor() as cur:
    cur.execute(f"PUT file://{file_path} @{stage_name}")

def load_json_data_from_stage(ctx, stage_name, table_name):
    with ctx.cursor() as cur:
        cur.execute(f"""
            COPY INTO {table_name} (id, name, start_date, end_date , data)
            FROM (SELECT $1:id::integer, $1:name::string, $1:start_date::datetime, $1:end_date::datetime, $1 FROM @{stage_name})
            FILE_FORMAT = (TYPE = 'JSON')
        """)

ctx = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT
    )
cs = ctx.cursor()
try:
    cs.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE}")
    cs.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
    cs.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE} ")
    cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    cs.execute(f"CREATE TEMPORARY STAGE {SNOWFLAKE_STAGE}")
    cs.execute(
        f"CREATE OR REPLACE TABLE {SNOWFLAKE_TABLE}(id integer, name string, start_date datetime, end_date datetime, data string)")
    file_name = './experiment.json'
    upload_file_to_stage(ctx,SNOWFLAKE_STAGE,file_name)
    load_json_data_from_stage(ctx, SNOWFLAKE_STAGE, SNOWFLAKE_TABLE)

    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

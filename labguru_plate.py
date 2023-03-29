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
SNOWFLAKE_TABLE = 'lg_plates_table'

def upload_file_to_stage(ctx,stage_name, file_path):
  with ctx.cursor() as cur:
    cur.execute(f"PUT file://{file_path} @{stage_name}")


def load_csv_data_from_stage(ctx, stage_name, file_path, table_name, hour, lg_plate_id, cell, target):
    cursor = ctx.cursor()
    try:
        # Build the SQL query to load the data from the stage into the table
        sql = f"""
            COPY INTO {table_name}
            FROM (SELECT null , null, null, $1, $2, $3 , null FROM '@{stage_name}/{os.path.basename(file_path)}')
            FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1);
        """
        
        # Execute the SQL query
        cursor.execute(sql)
    
        update_sql = f"""
            UPDATE {table_name}
            SET labguru_plate_id = {lg_plate_id}
            WHERE labguru_plate_id IS NULL;
        """

        # Update the hour column in the target table
        update_sql = f"""
            UPDATE {table_name}
            SET hour = {hour}
            WHERE hour IS NULL;
        """


        update_sql = f"""
            UPDATE {table_name}
            SET drug_candidate = '{target}'
            WHERE drug_candidate IS NULL;
        """

        update_sql = f"""
            UPDATE {table_name}
            SET cell_line = '{cell}'
            WHERE cell_line IS NULL;
        """
        
        cursor.execute(update_sql)
        
        cursor.close()
    except Exception as e:
        print(f"Error loading data from stage to table: {e}")
        cursor.close()
        raise e

print(SNOWFLAKE_USER)
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
        f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE}(labguru_plate_id string, cell_line string, drug_candidate string, well string, concentration number(10,3), readout number, sample_hour number)")
    for filename in os.listdir('plates_data'):
      if filename.endswith('.csv'):
        filepath = os.path.join('plates_data', filename)
        hour = int(filename.split('_')[3])
        print(hour) # Output: 4
        labguru_plate_id = 3
        # Upload the CSV file to the Snowflake stage
        upload_file_to_stage(ctx, SNOWFLAKE_STAGE, filepath)
        
        # Load the data from the Snowflake stage into the target table
        load_csv_data_from_stage(ctx, SNOWFLAKE_STAGE, filepath, SNOWFLAKE_TABLE,hour, labguru_plate_id, "HeLa", "Trg108" )


    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

from flask import Flask

import os
import json
import requests
import csv 
from snowflake.connector import connect
from snowflake.connector import DictCursor

from flask import request
from flask import jsonify
from flask import render_template

SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
SNOWFLAKE_DATABASE = os.environ['SNOWFLAKE_DATABASE']
SNOWFLAKE_SCHEMA = os.environ['SNOWFLAKE_SCHEMA']
SNOWFLAKE_TABLE = os.environ['SNOWFLAKE_TABLE']
SNOWFLAKE_STAGE = 'lg_temp'
SERVER= os.environ['LABGURU_SERVER']
TOKEN= os.environ['LABGURU_TOKEN']
app = Flask(__name__)


@app.route("/")
def hello_world():
    return render_template("index.html")

#to register this webhook use the following url
# 
@app.route("/plates")
def plate_upload(methods=['POST']):
    payload = request.json
    payload = payload[0] 
    print(f"Received payload: {payload}")
    api_url = payload['url']
    plate_data = requests.get(f'{SERVER}{api_url}?token={TOKEN}').json()
    file_name = plate_data["attachment_file_name"]
    if 'cell_growth_data_' in file_name:
        raw_data = requests.get(f'{SERVER}{api_url}/download?token={TOKEN}').json()
        path = save_csv_data_to_file(plate_data,file_name)
        ctx = get_ctx()
        labguru_plate_id = 3
        upload_file_to_stage(ctx,SNOWFLAKE_STAGE,path)
        hour = int(file_name.split('_')[3])
        load_csv_data_from_stage(ctx, SNOWFLAKE_STAGE, path, SNOWFLAKE_TABLE,hour, labguru_plate_id, "HeLa", "Trg108" )

@app.route('/receive_payload', methods=['POST'])
def receive_payload():
    payload = request.json
    payload = payload[0] 
    print(f"Received payload: {payload}")  # Add this line to print the received payload
    api_url = payload['url']
    # Retrieve experiment data
    
    experiment_data = requests.get(f'{SERVER}{api_url}?token={TOKEN}').json()

    # Retrieve element data
    for procedure in experiment_data.get('experiment_procedures', []):
        for element in procedure['experiment_procedure']['elements']:
            element_id = element['id']
            element_api_url = f'{SERVER}/api/v1/elements/{element_id}?token={TOKEN}'
            element_data = requests.get(element_api_url).json()
            element.update(element_data)

    # Save the merged data as a JSON file
    file_name = f"experiment_{experiment_data['id']}.json"
    with open(file_name, 'w') as file:
        json.dump(experiment_data, file)

    # Upload the JSON file to Snowflake
    upload_to_snowflake(file_name)

    # Delete the file after successful upload
    os.remove(file_name)

    return {'status': 'success'}

def save_csv_data_to_file(plate_data, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = plate_data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in plate_data:
            writer.writerow(row)

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

def update_table(cursor, table_name, column, value):
    update_sql = f"""
        UPDATE {table_name}
        SET {column} = {value}
        WHERE {column} IS NULL;
    """
    cursor.execute(update_sql)

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
    
        update_table(cursor, table_name, "labguru_plate_id", lg_plate_id)
        update_table(cursor, table_name, "hour", hour)
        update_table(cursor, table_name, "drug_candidate", f"'{target}'")
        update_table(cursor, table_name, "cell_line", f"'{cell}'")

        
        cursor.close()
    except Exception as e:
        print(f"Error loading data from stage to table: {e}")
        cursor.close()
        raise e

def get_ctx():
    snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT)

def upload_to_snowflake(file_name):
    ctx = get_ctx()
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
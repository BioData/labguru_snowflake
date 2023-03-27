from flask import Flask

import os
import json
import requests
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
SERVER= os.environ['LABGURU_SERVER']
TOKEN= os.environ['LABGURU_TOKEN']
app = Flask(__name__)


@app.route("/")
def hello_world():
    return render_template("index.html")

@app.route('/receive_payload', methods=['POST'])
def receive_payload():
    payload = request.json
    api_url = payload['url']
    # Retrieve experiment data
    experiment_data = requests.get(api_url).json()

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

def upload_to_snowflake(file_name):
    with connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    ) as con:
        with con.cursor(DictCursor) as cur:
            with open(file_name) as file:
                json_data = file.read()
                cur.execute(f"INSERT INTO {SNOWFLAKE_TABLE} (data) VALUES (PARSE_JSON(%s))", (json_data,))
                con.commit()
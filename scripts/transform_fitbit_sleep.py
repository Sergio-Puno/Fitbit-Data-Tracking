import json
import numpy as np
import pandas as pd
import datetime as dt
import psycopg2
import psycopg2.extras as extras

from scripts.db_config import config, create_connection, close_connection

# Get current time for processing
current_hour = int((dt.datetime.now() - dt.timedelta(hours=1)).strftime("%H")) - 7 # adjust to PST time UTC -7 hrs

def transform_data(**kwargs):
    # Grab extract task file information from XCOM
    task_instance = kwargs['ti']
    file_metadata = task_instance.xcom_pull(key='api_result_heatrate', task_ids='fitbit_extract')

    # Open raw file 
    f = open(file_metadata['Filename'])

    # Load into json object and close file
    raw_json = json.load(f)
    f.close()

    # Exrtract Minute-level data, remove log data from dictionary for further processing
    sleep_log = pd.DataFrame(raw_json['sleep'][0]['minuteData'])
    del raw_json['sleep'][0]['minuteData']

    # Extract Sleep details-level data
    sleep_details = pd.DataFrame([raw_json['sleep'][0]])

    # Extract Sleep-Summary-level data
    sum_df1 = pd.DataFrame([raw_json['summary']['stages']])
    del raw_json['summary']['stages']
    sum_df2 = pd.DataFrame([raw_json['summary']])
    sleep_summary = sum_df1.join(sum_df2)

    # Fixing data types
    dtype_sleep_log = {'dateTime': 'datetime64', 'value': 'int16'}
    sleep_log = sleep_log.astype(dtype_sleep_log)

    # Rename columns to match database

    # Move data into the staging table
    database_load(df, file_metadata)


def database_load(df, filename, **kwargs):
    # Establish connection
    conn = create_connection()

    # Create cursor
    cursor = conn.cursor()

    # Setup data for query load
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    sql_insert_query = "INSERT INTO %s(%s) VALUES %%s ;" % ('staging_heartrate', cols)

    # Attempt the database insert query
    try: 
        psycopg2.extras.execute_values(cursor, sql_insert_query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s'% error)
        conn.rollback()
        cursor.close()
        return 1

    close_connection(filename, cursor)

    print('Data Staging Complete')
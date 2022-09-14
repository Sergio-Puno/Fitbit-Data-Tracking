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

    f = open(file_metadata['Filename'])

    raw_json = json.load(f)
    heartrate_data = pd.DataFrame(raw_json['activities-heart-intraday']['dataset'])
    f.close()

    # Fixing data types
    heartrate_data['time'] = heartrate_data['time'].astype('datetime64')
    heartrate_data['value'] = heartrate_data['value'].astype('int16')

    # Parse out the last hour of data only from this dataframe
    last_hour_data = heartrate_data.loc[heartrate_data['time'].dt.hour == current_hour, ['time', 'value']]
    #print(df[df['time'].dt.hour == current_hour])
    last_hour_data.reset_index(drop=True, inplace=True)

    # Rename columns to match database
    last_hour_data.rename(columns={'time': 'timestamp', 'value': 'heartrate'}, inplace=True)

    # Move data into the staging table
    database_load(last_hour_data, file_metadata)


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
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import datetime as dt

from scripts.db_config import config


def create_connection():
    params = config()
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    return conn

def close_connection(filename, cursor):
    print('File updated to table:', filename)
    cursor.close()

def database_load(**kwargs):
    # Establish connection
    conn = create_connection()

    # Grab extract task file information from XCOM
    task_instance = kwargs['ti']
    file_metadata = task_instance.xcom_pull(key='api_result_sleep', task_ids='fitbit_extract')

    filename = file_metadata['Filename']

    # Open file from prior task
    df = pd.read_csv(filename)

    # Create cursor
    cursor = conn.cursor()

    # Setup data for query load
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    sql_insert_query = "INSERT INTO %s(%s) VALUES %%s ;" % ('sleep_log', cols)

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
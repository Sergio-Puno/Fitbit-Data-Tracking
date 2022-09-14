import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import datetime as dt

from scripts.db_config import config, create_connection, close_connection


def database_load(**kwargs):
    # Establish connection
    conn = create_connection()
    cursor = conn.cursor()

    # Query staging table for table to load into the base table
    sql_extract_query = "SELECT * FROM staging_heartrate"
    cursor.execute(sql_extract_query)
    rows = cursor.fetchall()

    heartrate_data = pd.DataFrame(rows, columns=['timestamp', 'heartrate'])

    # Setup data for query load
    tuples = [tuple(x) for x in heartrate_data.to_numpy()]
    cols = ','.join(list(heartrate_data.columns))

    sql_insert_query = "INSERT INTO %s(%s) VALUES %%s ;" % ('heartrate', cols)

    # Attempt the database insert query
    try: 
        psycopg2.extras.execute_values(cursor, sql_insert_query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s'% error)
        conn.rollback()
        cursor.close()
        return 1

    close_connection('Staging_heartrate', cursor)

    # UseAirflow XCOM to pass the table name for stage table cleaning
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='staging_table', value='staging_heartrate')

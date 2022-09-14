from scripts.db_config import config, create_connection, close_connection

def clean_staging(**kwargs):
    # Establish connection
    conn = create_connection()
    cursor = conn.cursor()

    # Grab extract task file information from XCOM
    task_instance = kwargs['ti']
    table = task_instance.xcom_pull(key='staging_table', task_ids='fitbit_load')

    # Query staging table for table to load into the base table
    sql_extract_query = "DELETE FROM {}".format(table)
    cursor.execute(sql_extract_query)
    conn.commit()

    close_connection('Staging_heartrate', cursor)

    print('Staging table for {} cleared.'.format(table))



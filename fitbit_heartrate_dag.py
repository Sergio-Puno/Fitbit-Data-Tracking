# import airflow DAG and operator
from airflow import DAG
from airflow.operators.python import PythonOperator

# import modules
import pendulum

# import custom scripts
from scripts.extract_fitbit_heartrate import extract_data
from scripts.transform_fitbit_heartrate import transform_data
from scripts.load_fitbit_heartrate import database_load
from scripts.clean_fitbit_staging import clean_staging

with DAG(
    'fitbit_heartrate_dag',
    description='Fitbit Heartrate Data ETL',
    start_date=pendulum.datetime(2022, 7, 1, tz='UTC'),
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['fitbit', 'api', 'heartrate']
) as dag:
    fitbit_extract = PythonOperator(
        task_id='fitbit_extract',
        python_callable=extract_data,
        provide_context=True
    )

    fitbit_transform = PythonOperator(
        task_id='fitbit_transform',
        python_callable=transform_data,
        provide_context=True
    )

    fitbit_load = PythonOperator(
        task_id='fitbit_load',
        python_callable=database_load,
        provide_context=True
    )

    fitbit_clean_staging = PythonOperator(
        task_id='fitbit_clean_staging',
        python_callable=clean_staging,
        provide_context=True
    )

    fitbit_extract >> fitbit_transform >> fitbit_load >> fitbit_clean_staging
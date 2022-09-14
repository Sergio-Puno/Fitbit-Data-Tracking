# import airflow DAG and operator
from airflow import DAG
from airflow.operators.python import PythonOperator

# import modules
import pendulum

# import custom scripts
from scripts.extract_fitbit_sleep import extract_data
from scripts.load_fitbit_sleep import database_load

with DAG(
    'fitbit_sleep_dag',
    description='Fitbit Sleep Data ETL',
    start_date=pendulum.datetime(2022, 7, 1, tz='UTC'),
    schedule_interval='30 9 * * *',
    catchup=False,
    tags=['fitbit', 'api', 'sleep']
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

#    fitbit_load = PythonOperator(
#        task_id='fitbit_load',
#        python_callable=database_load,
#        provide_context=True
#    )
#
#    fitbit_extract >> fitbit_load
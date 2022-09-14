# import airflow DAG and operator
from datetime import timedelta
from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pendulum
import configparser

# Get timestamp in PST
timestamp_now = str((pendulum.now() - timedelta(hours=7)).strftime("%Y-%m-%d %H:%M"))

parser = configparser.ConfigParser()
parser.read("./dags/files/pipeline.conf")
slack_token = parser.get('slack', 'token')

def get_message() -> str:
    return "Please load Fitbit data within the next 10 min: " + timestamp_now + " PST"

with DAG(
    "fitbit_alerts",
    start_date = pendulum.datetime(2022, 1, 1, tz="UTC"),
    schedule_interval="15 7 * * *",
    catchup=False,
    tags=['alerts', 'fitbit']
) as dag:

    send_slack_notification = SlackWebhookOperator(
        task_id='send_slack_notification',
        http_conn_id='slack_conn',
        webhook_token=slack_token,
        message=get_message(),
        channel='#airflowalerts'
    )
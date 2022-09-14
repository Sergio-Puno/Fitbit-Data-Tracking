import configparser
import pandas as pd
import datetime as dt
import requests
import json

# Get date / current time for processing
# Note: timing is based on Airflow, thus UTC. Timings converted to PST 
data_hour = (dt.datetime.now() - dt.timedelta(hours=8)).strftime("%Y-%m-%d_%H-%M")
current_hour = int((dt.datetime.now() - dt.timedelta(hours=1)).strftime("%H")) - 7 # adjust to PST time UTC -7 hrs

def parse_credentials():
    # Import credentials from config file
    parser = configparser.ConfigParser()
    parser.read("./dags/files/pipeline.conf")
    CLIENT_ID = parser.get('fitbit', 'oauth_client_id')
    CLIENT_SECRET = parser.get('fitbit', 'client_secret')
    API_ID = parser.get('fitbit', 'api_id')
    ACCESS_TOKEN = parser.get('fitbit', 'access_token')

    return CLIENT_ID, CLIENT_SECRET, API_ID, ACCESS_TOKEN

def dump_data(json_file, filename):
    # Dump last hour data into file store (local folder as json file)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_file, f, ensure_ascii=True, indent=4)

def extract_data(**kwargs):
    print("Today's date:", date_today)
    print("Current hour:", current_hour)

    CLIENT_ID, CLIENT_SECRET, API_ID, ACCESS_TOKEN = parse_credentials()

    # Get heart rate data for the last hour
    # This involves downloading the entire day's data, then parsing out just the last hour

    # TODO
    # Might be able to just request the last hour if i have a log of the timestamps this runs
    # https://dev.fitbit.com/build/reference/web-api/intraday/get-heartrate-intraday-by-date/
    activity_request = requests.get('https://api.fitbit.com/1/user/' + API_ID + '/sleep/date/today.json',
                                        headers={'Authorization': 'Bearer ' + ACCESS_TOKEN})
    
    # Debug
    print('#######################')
    print('Status Code: ', activity_request.status_code)
    print('#######################')

    oneNightData = activity_request.json()

    # Call data dump function to write out to JSON in specified directory
    filename = "./dags/files/files_sleep/sleep_log_" + data_hour + ".json"
    dump_data(oneNightData, filename)

    # Create dict with file details to pass onto the load task
    process_file = {
        'Timestamp': data_hour,
        'Filename': filename
    }

    # UseAirflow XCOM to pass the dataframe into the load task for PostgreSQL
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='api_result_sleep', value=process_file)
# Fitbit Data Tracking Using Airflow

This personal project does the following:

- Sends the user a reminder via Slack to update their Fitbit data using the app on their phone (this data is not automatically sync'd)
- Once the top of a new hour is reached, various DAGs will be triggered and download data from the Fitbit API 
- This data is stored in several steps:
  - Raw data copy is stored locally as JSON
  - Light transformations (column renaming, data type validation) stored into a staging database table 
  - Final transformations (computations, aggregations, enrichment) stored into a local database table
- Once data has been stored and DAGs completed, a secondary set of DAGs kick off the analytics portion providing insight into the user's activity and suggestions for the next hour

Tools used in this project:

- Python / SQL
- PostgreSQL (local instance)
- Airflow
- Fitbit API
- Slack (webhook for notifications)


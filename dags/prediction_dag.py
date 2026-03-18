import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# Set to run every 2 mins. catchup=False so it doesn't try to run 100 times on start.
with DAG(
    dag_id="prediction_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False
) as dag:

    @task
    def check_for_new_data():
        # folder where we drop the files
        data_dir = "/opt/airflow/data/good_data/"
        
        # simple check to make sure the folder actually exists
        if not os.path.exists(data_dir):
            raise AirflowSkipException(f"Folder {data_dir} is missing.")

        # find all csvs
        files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
        
        # If nothing is there, just skip the run (turns the task pink in UI)
        if len(files) == 0:
            raise AirflowSkipException("No new files found. Nothing to do.")
            
        return files

    @task
    def make_predictions(csv_list):
        import pandas as pd
        import requests
        
        data_dir = "/opt/airflow/data/good_data/"
        # Uses the container name from docker-compose
        api_endpoint = "http://fastapi-service:80/predict"
        
        for file in csv_list:
            path = os.path.join(data_dir, file)
            
            try:
                # Load the data and convert to list of dicts for the API
                df = pd.read_csv(path)
                payload = df.to_dict(orient="records")
                
                # Push to FastAPI with a 10s timeout so it doesnt hang forever
                res = requests.post(api_endpoint, json=payload, timeout=10)
                
                # This throws an error if the API returns 4xx or 5xx
                res.raise_for_status()
                
                print(f"Worked: {file} sent. Status: {res.status_code}")
                
            except requests.exceptions.RequestException as err:
                # Catches connection issues, 404s, or if the server is down
                print(f"API Error for {file}: {err}")
                raise
                
            except Exception as other_err:
                # Catches weird stuff like empty CSVs or bad formatting
                print(f"Failed to process {file}: {other_err}")
                raise

    # Flow: Check -> Predict
    new_files = check_for_new_data()
    make_predictions(new_files)
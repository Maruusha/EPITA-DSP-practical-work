import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# Define the DAG to run every 2 minutes automatically
with DAG(
    dag_id="prediction_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False
) as dag:

    @task
    def check_for_new_data():
        # Path inside the Docker container
        folder_path = "/opt/airflow/data/good_data/"
        
        # Get the list of all files
        all_files = os.listdir(folder_path)
        
        # Keep only CSV files
        csv_files = []
        for file_name in all_files:
            if file_name.endswith(".csv"):
                csv_files.append(file_name)
                
        n = len(csv_files)
        
        # Mark the DAG run as skipped if empty
        if n == 0:
            raise AirflowSkipException("No new data found. Skipping prediction.")
            
        return csv_files

    @task
    def make_predictions(file_list):
        # Move imports INSIDE the task so Airflow doesn't crash during the scan
        import pandas as pd
        import requests
        
        folder_path = "/opt/airflow/data/good_data/"
        
        # Updated to match the exact container name and internal port from Docker Desktop
        api_url = "http://fastapi-service:80/predict"
        
        for file_name in file_list:
            file_path = folder_path + file_name
            
            # Read the CSV file into a dictionary
            df = pd.read_csv(file_path)
            data_dict = df.to_dict(orient="records")
            
            # Make the API call to Duy Thai's FastAPI service
            response = requests.post(api_url, json=data_dict)
            
            # Print the status so you can see it in the Airflow logs
            print("Sent file: " + file_name + " - Status: " + str(response.status_code))

    # Link the tasks together to form the pipeline
    files_to_process = check_for_new_data()
    make_predictions(files_to_process)
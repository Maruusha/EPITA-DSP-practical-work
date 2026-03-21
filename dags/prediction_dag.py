import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# Define the DAG
with DAG(
    dag_id="prediction_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False
) as dag:

    @task
    def check_for_new_data():
        # Set up the official logger
        logger = logging.getLogger(__name__)
        
        data_dir = "/opt/airflow/data/good_data/"
        
        if not os.path.exists(data_dir):
            raise AirflowSkipException("Folder " + data_dir + " is missing.")

        files = []
        for f in os.listdir(data_dir):
            if f.endswith(".csv"):
                files.append(f)
        
        if len(files) == 0:
            logger.info("No new files found. Skipping.")
            raise AirflowSkipException("No new files found. Nothing to do.")
            
        return files

    @task
    def make_predictions(csv_list):
        import pandas as pd
        import requests
        
        # Set up the logger inside the task
        logger = logging.getLogger(__name__)
        
        data_dir = "/opt/airflow/data/good_data/"
        api_endpoint = "http://fastapi-service:80/predict"
        
        for file in csv_list:
            path = os.path.join(data_dir, file)
            
            try:
                # Load the CSV
                df = pd.read_csv(path)
                
                # --- THE 422 ERROR FIX ---
                # Rename the columns to perfectly match the API's expected format.
                # Note: If your CSV has slightly different names (like "date" instead of "Date"), 
                # adjust the left side of these pairs.
                df = df.rename(columns={
                    "Date": "date",
                    "Start_Hour": "start_hour",
                    "End_Hour": "end_hour",
                    "Source": "energy_source"
                })
                # -------------------------

                # Convert to the JSON list format the API needs
                payload = df.to_dict(orient="records")
                
                # Send to the API
                res = requests.post(api_endpoint, json=payload, timeout=10)
                res.raise_for_status()
                
                # Log success
                logger.info("Worked: " + file + " sent. Status: " + str(res.status_code))
                
            except requests.exceptions.RequestException as err:
                # Log API/Connection errors and fail the task
                logger.error("API Error for " + file + ": " + str(err))
                raise
                
            except Exception as other_err:
                # Log any other weird errors (like bad CSV formats)
                logger.error("Failed to process " + file + ": " + str(other_err))
                raise

    # Flow
    new_files = check_for_new_data()
    make_predictions(new_files)
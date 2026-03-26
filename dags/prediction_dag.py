import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from airflow.exceptions import AirflowSkipException

with DAG(
    dag_id="prediction_job",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False
) as dag:

    @task
    def check_for_new_data():
        logger = logging.getLogger(__name__)
        data_dir = "/opt/airflow/data/good_data/"
        processed_log = "/opt/airflow/data/good_data/processed_files.txt"

        if not os.path.exists(data_dir):
            raise AirflowSkipException(f"{data_dir} is missing")

        files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

        if len(files) == 0:
            raise AirflowSkipException("No files found")

        # Load processed files
        if os.path.exists(processed_log):
            with open(processed_log, "r") as f:
                done_files = set(f.read().splitlines())
        else:
            done_files = set()

        # Keep only new files
        new_files = [f for f in files if f not in done_files]

        if len(new_files) == 0:
            logger.info("No NEW files to process")
            raise AirflowSkipException("Nothing new")

        return new_files


    @task
    def make_predictions(files_to_process):
        import pandas as pd
        import requests

        logger = logging.getLogger("airflow.task")

        folder_path = "/opt/airflow/data/good_data/"
        processed_log = "/opt/airflow/data/good_data/processed_files.txt"
        api_endpoint = "http://fastapi:80/predict"

        batch_size = 100

        for filename in files_to_process:
            filepath = os.path.join(folder_path, filename)

            if not os.path.exists(filepath):
                continue

            try:
                df = pd.read_csv(filepath)

                df = df.rename(columns={
                    "Date": "date",
                    "Start_Hour": "start_hour",
                    "End_Hour": "end_hour",
                    "Source": "energy_source"
                })

                df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')
                df = df[df["start_hour"] != 23]
                df["prediction_source"] = "Scheduled"
                df = df.dropna()

                # 🔥 BATCHING (fix API error)
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]
                    payload = batch_df.to_dict(orient="records")

                    res = requests.post(api_endpoint, json=payload)

                    if res.status_code == 422:
                        logger.error(f"❌ DATA REJECTED: {res.text}")

                    res.raise_for_status()

                # ✅ ONLY mark file as done AFTER full success
                with open(processed_log, "a") as f:
                    f.write(filename + "\n")

                logger.info(f"✅ Finished processing: {filename}")

            except Exception as e:
                logger.error(f"❌ Error processing {filename}: {e}")
                raise


    found_files = check_for_new_data()
    make_predictions(found_files)

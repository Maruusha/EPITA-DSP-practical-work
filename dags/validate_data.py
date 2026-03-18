import logging
from datetime import datetime
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id='validate_data',
    description='Ingest data from a file in raw_data folder to another DAG',
    tags=['dsp', 'data_ingestion', 'validate_data'],
    schedule=timedelta(minutes=5),
    start_date=pendulum.today("UTC"),  # sets the starting point of the DAG
    max_active_runs=1  # Ensure only one active run at a time
)
def validate_data():
    @task
    def read_data() -> list[dict]:    
        folderpath = '../data/raw_data/'
        input_data_df = pd.read_csv(filepath)
        logging.info(f'Extract {nb_rows} rows from the file {filepath}')
        data_to_ingest_df = input_data_df.sample(n=nb_rows)
        return data_to_ingest_df.to_dict(orient="records")

    @task
    def validate_data(data_to_ingest: list[dict]) -> None:
        filepath = f'output_data/{datetime.now().strftime("%Y-%M-%d_%H-%M-%S")}.csv'
        logging.info(f'Ingesting data to the file: {filepath}')
        pd.DataFrame(data_to_ingest).to_csv(filepath, index=False)

    # TODO - Dev task 
    @task
    def save_statistics() -> None:
        pass

    # TODO - Sapal task 
    @task
    def send_alerts() -> None:
        pass

    @task
    def split_and_save_data() -> None:
        pass 

    # Task relateionships
    data_to_ingest = read_data()
    temp = validate_data(data_to_ingest)
    # save_statistics(temp)
    # send_alerts(send_alerts)
    # split_and_save_data(send_alerts)

validate_data()
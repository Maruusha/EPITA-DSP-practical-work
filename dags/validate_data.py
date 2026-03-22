import logging
import os
import random
from datetime import datetime
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id='validate_data',
    description='Ingest data from a file in raw_data folder, validate and process it',
    tags=['dsp', 'data_ingestion', 'validate_data'],
    schedule=timedelta(minutes=5),
    start_date=pendulum.today("UTC"),  # sets the starting point of the DAG
    max_active_runs=1  # Ensure only one active run at a time
)
def validate_data():
    @task
    def read_data() -> list[dict]:    
        folderpath = '../data/raw_data/'
        csv_files = [f for f in os.listdir(folderpath) if f.endswith('.csv')]
        if csv_files:
            filepath = random.choice(csv_files)
            full_path = os.path.join(folderpath, filepath)

            input_data_df = pd.read_csv(full_path)
            logging.info(f'Extract {input_data_df.size} rows from the file {filepath}')
            
            data_to_ingest_df = input_data_df.sample(n=input_data_df.size)
            return data_to_ingest_df.to_dict(orient="records")
        else: 
            return None

    @task
    def validate_data(data_to_ingest: list[dict]) -> list[dict]:
        if not data_to_ingest:
            return None
        
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
    def split_and_save_data(data_to_ingest: list[dict]) -> None:
        if not data_to_ingest:
            return 
        # Good
        filepath = '../data/good_data/'

        # Bad
        filepath = '../data/bad_data/'


        file_name = f'{filepath}{datetime.now().strftime("%Y-%M-%d_%H-%M-%S")}.csv'
        logging.info(f'Ingesting data to the file: {filepath}')
        pd.DataFrame(data_to_ingest).to_csv(filepath, index=False)

    # Task relateionships
    data_to_ingest = read_data()
    file_content = validate_data(data_to_ingest)
    # save_statistics(temp)
    # send_alerts(temp)
    split_and_save_data(file_content)

validate_data()
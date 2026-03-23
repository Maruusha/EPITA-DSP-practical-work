import logging
import os
import random
from datetime import datetime, timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task
import great_expectations as gx

from DataValClass import DataValClass

# Reference lists for validation
VALID_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
VALID_MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
VALID_SEASONS = ['Spring', 'Summer', 'Fall', 'Winter']
VALID_SOURCES = ['Wind', 'Solar', 'Mixed'] 

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
    def read_data() -> dict:    
        folderpath = '../data/raw_data/'
        csv_files = [f for f in os.listdir(folderpath) if f.endswith('.csv')]
        if csv_files:
            filename = random.choice(csv_files)
            full_path = os.path.join(folderpath, filename)

            input_data_df = pd.read_csv(full_path)
            logging.info(f'Extract {input_data_df.size} rows from the file {filename}')

            # Handle empty file
            if input_data_df.size < 1:
                return {}
            
            data_to_ingest_df = input_data_df.sample(n=input_data_df.size)
            data_obj = DataValClass(
                        records=data_to_ingest_df.to_dict(orient="records"),
                        source_filename=filename,
                        is_processed=False,
                        total_rows=len(data_to_ingest_df)
                    )

            return data_obj.model_dump() 
        else: 
            return {}

    @task
    def validate_data(data_to_ingest: dict) -> dict:
        if not data_to_ingest:
            return {}
        
        payload = DataValClass(**data_to_ingest)
        df = pd.DataFrame(payload.records)

        # Initialize GX Context
        context = gx.get_context()
        datasource = context.sources.add_pandas(name="my_pandas_datasource")
        asset = datasource.add_dataframe_asset(name="my_df_asset")

        # Define Expectations Rules
        batch_request = asset.build_batch_request(dataframe=df)
        expectation_suite_name = "dsp_validation_suite"
        context.add_or_update_expectation_suite(expectation_suite_name)
        
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
        )

        full_col_list = ['Date', 'Start_Hour', 'End_Hour', 'Source', 'Day_of_Year', 'Day_Name',	'Month_Name', 'Season', 'Production']

        # Schema / Missing Columns
        schema_res = validator.expect_table_columns_to_match_set(column_set=full_col_list)

        # Completeness, Type
        for col in full_col_list:
            validator.expect_column_values_to_not_be_null(column=col) # Completeness
            # Type (Columns should be numeric stay numeric), 'Validity' (There is no xyz day)
            if col in ['Start_Hour', 'End_Hour', 'Day_of_Year', 'Production']:
                validator.expect_column_values_to_be_in_type_list(
                    column=col, type_list=['int', 'float', 'int64', 'float64']
                )
        validator.expect_column_values_to_be_in_set(column='Day_Name', value_set=VALID_DAYS)
        validator.expect_column_values_to_be_in_set(column='Month_Name', value_set=VALID_MONTHS)
        validator.expect_column_values_to_be_in_set(column='Season', value_set=VALID_SEASONS)
        validator.expect_column_values_to_be_in_set(column='Source', value_set=VALID_SOURCES)

        # Outlier, ImpossibleValue
        validator.expect_column_values_to_be_between(column="Start_Hour", min_value=0, max_value=24) 
        validator.expect_column_values_to_be_between(column="End_Hour", min_value=0, max_value=24) 
        validator.expect_column_values_to_be_between(column="Day_of_Year", min_value=1, max_value=366) 
        validator.expect_column_values_to_be_between(column="Production", min_value=0, max_value=25000) # Max value is 23k
        validator.expect_column_values_to_match_strftime_format(column="Date", strftime_format="%m/%d/%Y")
        # Using pendulum for timezone consistency with Airflow
        now_str = pendulum.now("UTC").strftime('%m/%d/%Y')
        validator.expect_column_values_to_be_between(
            column="Date",
            min_value="01/01/2018",       
            max_value=now_str,            # Current date
            parse_strings_as_datetime=True,
            allow_cross_type_comparisons=True
        )

        # Criticality calculation
        results = validator.validate()

        # Use a SET to store unique row indices that failed
        bad_indices = set()
        for r in results.results:
            if not r.success:
                # GX returns 'unexpected_index_list' if the data is a DataFrame
                indices = r.result.get('unexpected_index_list', [])
                bad_indices.update(indices)
        payload.error_count = len(bad_indices)
        error_rate = payload.error_count / payload.total_rows

        # Determine Criticality based on rules
        if not schema_res.success or error_rate > 0.50:
            payload.error_criticality = "High"
        elif 0.10 <= error_rate <= 0.50:
            payload.error_criticality = "Medium"
        elif 0 < error_rate < 0.10:
            payload.error_criticality = "Low"
        else:
            payload.error_criticality = "None"

        payload.is_schema_valid = schema_res.success
        payload.is_processed = True

        # Split the DataFrame using the indices
        bad_df = df.iloc[list(bad_indices)]
        good_df = df.drop(index=list(bad_indices))
        payload.bad_records = bad_df.to_dict(orient="records")
        payload.good_records = good_df.to_dict(orient="records")
     
        return payload.model_dump() 

    # TODO - Dev task 
    @task
    def save_statistics(data_to_ingest: dict) -> None:
        if not data_to_ingest:
            return {}
        pass

    # TODO - Sapal task 
    @task
    def send_alerts(data_to_ingest: dict) -> None:
        if not data_to_ingest:
            return {}
        pass

    @task
    def split_and_save_data(data_to_ingest: dict) -> None:
        if not data_to_ingest:
            logging.warning("No data received in split_and_save_data.")
            return 
        
        payload = DataValClass(**data_to_ingest)
        good_folder = '../data/good_data/'
        bad_folder = '../data/bad_data/'
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_name = payload.source_filename.replace('.csv', '')

        # Save good records to good_data
        if payload.good_records:
            os.makedirs(good_folder, exist_ok=True) # Ensure folder exists
            good_filepath = f"{good_folder}{base_name}_{timestamp}.csv"
            pd.DataFrame(payload.good_records).to_csv(good_filepath, index=False)
            logging.info(f"Successfully saved {len(payload.good_records)} rows to: {good_filepath}")
        else:
            logging.info("No good records found to save.")

        # Save bad records to bad_data
        if payload.bad_records:
            os.makedirs(bad_folder, exist_ok=True) # Ensure folder exists     
            bad_filepath = f"{bad_folder}{base_name}_{timestamp}.csv"
            pd.DataFrame(payload.bad_records).to_csv(bad_filepath, index=False)
            logging.warning(f"Saved {len(payload.bad_records)} rows with errors to: {bad_filepath}")
        else:
            logging.info("No bad records found. Data is 100% clean.")

    # Task relateionships
    data_to_ingest = read_data()
    file_content = validate_data(data_to_ingest)
    # save_statistics(temp)
    # send_alerts(temp)
    split_and_save_data(file_content)

validate_data()
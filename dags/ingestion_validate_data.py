import logging
import os
import random
from datetime import datetime, timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task
import great_expectations as gx
import great_expectations.expectations as gxe

from DataValClass import DataValClass

# Reference lists for validation
VALID_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
VALID_MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
VALID_SEASONS = ['Spring', 'Summer', 'Fall', 'Winter']
VALID_SOURCES = ['Wind', 'Solar', 'Mixed'] 
COLUMNS = ['Date', 'Start_Hour', 'End_Hour', 'Source', 'Day_of_Year', 'Day_Name', 'Month_Name', 'Season', 'Production']

@dag(
    dag_id='ingestion_validate_data',
    description='Ingest data from a file in raw_data folder, validate and process it',
    tags=['dsp', 'data_ingestion', 'ingestion_validate_data'],
    schedule=timedelta(minutes=5),
    start_date=pendulum.today("UTC"),  # sets the starting point of the DAG
    max_active_runs=1  # Ensure only one active run at a time
)
def ingestion_validate_data():
    @task
    def read_data() -> dict:    
        folderpath = '/opt/airflow/data/raw_data/'
        csv_files = [f for f in os.listdir(folderpath) if f.endswith('.csv')]
        if csv_files:
            filename = random.choice(csv_files)
            full_path = os.path.join(folderpath, filename)

            data_to_ingest_df = pd.read_csv(full_path)
            df_row_len = data_to_ingest_df.shape[0]
            logging.info(f'Extract {df_row_len} rows from the file {filename}')

            # Handle empty file
            if df_row_len < 1:
                return {}
            
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

        context = gx.get_context()
        suite_name = "dsp_validation_suite"

        try:
            suite = context.suites.get(name=suite_name)
        except:
            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

        suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=COLUMNS))
        for col in COLUMNS:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        for col in ['Start_Hour', 'End_Hour', 'Day_of_Year', 'Production']:
            suite.add_expectation(gxe.ExpectColumnValuesToBeInTypeList(
                column=col, type_list=['int', 'float', 'int64', 'float64']
            ))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Day_Name', value_set=VALID_DAYS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Month_Name', value_set=VALID_MONTHS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Season', value_set=VALID_SEASONS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Source', value_set=VALID_SOURCES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Start_Hour", min_value=0, max_value=24))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="End_Hour", min_value=0, max_value=24))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Day_of_Year", min_value=1, max_value=366))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Production", min_value=0, max_value=25000))
        suite.add_expectation(gxe.ExpectColumnValuesToMatchStrftimeFormat(column="Date", strftime_format="%m/%d/%Y"))

        datasource = context.data_sources.add_pandas(name="my_pandas_datasource")
        asset = datasource.add_dataframe_asset(name="my_df_asset")
        batch_definition = asset.add_batch_definition_whole_dataframe("my_batch")

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name="my_validation",
                data=batch_definition,
                suite=suite,
            )
        )
        results = validation_definition.run(batch_parameters={"dataframe": df})

        bad_indices = set()
        payload.is_schema_valid = True
        for r in results.results:
            if not r.success:
                indices = r.result.get('unexpected_index_list', [])
                bad_indices.update(indices)
            if r.expectation_config.type == "expect_table_columns_to_match_set":
                payload.is_schema_valid = r.success

        payload.error_count = len(bad_indices)
        payload.error_rate = payload.error_count / payload.total_rows

        if not payload.is_schema_valid or payload.error_rate > 0.50:
            payload.error_criticality = "High"
        elif 0.10 <= payload.error_rate <= 0.50:
            payload.error_criticality = "Medium"
        elif 0 < payload.error_rate < 0.10:
            payload.error_criticality = "Low"
        else:
            payload.error_criticality = "None"

        payload.is_processed = True
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
        good_folder = '/opt/airflow/data/good_data/'
        bad_folder = '/opt/airflow/data/bad_data/'
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_name = payload.source_filename.replace('.csv', '')

        summary = (
            f"source_filename:       {payload.source_filename}\n"
            f"is_processed:          {payload.is_processed}\n"
            f"is_schema_valid:       {payload.is_schema_valid}\n"
            f"schema_missing_column: {payload.schema_missing_column}\n"
            f"error_rate:            {payload.error_rate}\n"
            f"error_criticality:     {payload.error_criticality}\n"

            f"total_rows:            {payload.total_rows}\n"
            f"good_record_count:     {payload.total_rows-payload.error_count}\n"
            f"bad_record_count:      {payload.error_count}\n"
            
        )

        # Save good records to good_data
        if payload.good_records:
            os.makedirs(good_folder, exist_ok=True) # Ensure folder exists
            good_filepath = f"{good_folder}{base_name}_{timestamp}.csv"
            pd.DataFrame(payload.good_records).to_csv(good_filepath, index=False)
            logging.info(f"Successfully saved {len(payload.good_records)} rows to: {good_filepath}")
            # Write one summary file alongside the CSVs
            summary_filepath = f"{good_folder}{base_name}_{timestamp}.txt"
            with open(summary_filepath, 'w') as f:
                f.write(summary)
            logging.info(f"Summary saved to: {summary_filepath}")
        else:
            logging.info("No good records found to save.")

        # Save bad records to bad_data
        if payload.bad_records:
            os.makedirs(bad_folder, exist_ok=True) # Ensure folder exists     
            bad_filepath = f"{bad_folder}{base_name}_{timestamp}.csv"
            pd.DataFrame(payload.bad_records).to_csv(bad_filepath, index=False)
            logging.warning(f"Saved {len(payload.bad_records)} rows with errors to: {bad_filepath}")
            # Write one summary file alongside the CSVs
            summary_filepath = f"{bad_folder}{base_name}_{timestamp}.txt"
            with open(summary_filepath, 'w') as f:
                f.write(summary)
            logging.info(f"Summary saved to: {summary_filepath}")
        else:
            logging.info("No bad records found. Data is 100% clean.")

    # Task relateionships
    raw_data_to_ingest = read_data()
    file_content = validate_data(raw_data_to_ingest)
    save_statistics(file_content)
    send_alerts(file_content)
    split_and_save_data(file_content)

ingestion_validate_data()
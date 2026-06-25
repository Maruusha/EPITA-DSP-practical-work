import logging
import os
import random
from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

import great_expectations as gx
import great_expectations.expectations as gxe
import requests

from DataValClass import DataValClass

# Reference lists for validation
VALID_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
VALID_MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
VALID_SEASONS = ['Spring', 'Summer', 'Fall', 'Winter']
VALID_SOURCES = ['Wind', 'Solar', 'Mixed']
COLUMNS = ['Date', 'Start_Hour', 'End_Hour', 'Source', 'Day_of_Year', 'Day_Name', 'Month_Name', 'Season', 'Production']

# Map GX expectation types to error categories
EXPECTATION_CATEGORY_MAP = {
    'expect_column_values_to_not_be_null': 'completeness',
    'expect_column_values_to_be_between': 'validity',
    'expect_column_values_to_be_in_set': 'consistency',
    'expect_column_values_to_be_of_type': 'type',
    'expect_table_columns_to_match_set': 'schema',
    'expect_column_to_exist': 'schema',
}

@dag(
    dag_id='ingestion_validate_data_v1',
    description='Ingest data from a file in raw_data folder, validate and process it',
    tags=['dsp', 'data_ingestion', 'ingestion_validate_data'],
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False
)
def ingestion_validate_data():
    @task
    def read_data() -> dict:
        folderpath = '/opt/airflow/data/raw_data/'
        csv_files = [f for f in os.listdir(folderpath) if f.endswith('.csv')]
        if csv_files:
            filename = random.choice(csv_files)
            full_path = os.path.join(folderpath, filename)

            data_to_ingest_df = pd.read_csv(full_path, dtype=str)
            data_to_ingest_df = data_to_ingest_df.astype(object).fillna('nan_value')
            df_row_len = data_to_ingest_df.shape[0]
            logging.warning(f'Extract {df_row_len} rows from the file {filename}')

            os.remove(full_path)

            if df_row_len < 1:
                raise AirflowSkipException("File empty!!")

            data_obj = DataValClass(
                records=data_to_ingest_df.to_dict(orient="records"),
                source_filename=filename,
                is_processed=False,
                total_rows=len(data_to_ingest_df)
            )
            dump = data_obj.model_dump()
            logging.warning("Created the data_obj" + str(dump))
            return dump
        else:
            logging.warning("No CSV files found in raw_data folder. Skipping run.")
            raise AirflowSkipException("No file found!!")

    @task
    def validate_data(data_to_ingest: dict) -> dict:
        if not data_to_ingest:
            return {}

        payload = DataValClass(**data_to_ingest)
        df = pd.DataFrame(payload.records)

        # Convert columns
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
        numeric_cols = ['Start_Hour', 'End_Hour', 'Day_of_Year', 'Production']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        context = gx.get_context()
        suite_name = "dsp_validation_suite"

        try:
            suite = context.suites.get(name=suite_name)
        except:
            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

        suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=COLUMNS, exact_match=True))
        for col in COLUMNS:
            suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Day_Name', value_set=VALID_DAYS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Month_Name', value_set=VALID_MONTHS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Season', value_set=VALID_SEASONS))
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column='Source', value_set=VALID_SOURCES))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Start_Hour", min_value=0, max_value=24))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="End_Hour", min_value=0, max_value=24))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Day_of_Year", min_value=1, max_value=366))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Production", min_value=0, max_value=25000))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="Date", min_value=datetime(2018, 1, 1), max_value=datetime.now()))

        datasource = context.data_sources.add_or_update_pandas(name="my_pandas_datasource")
        try:
            asset = datasource.get_asset(name="my_df_asset")
            logging.info("Found existing GX Asset.")
        except:
            asset = datasource.add_dataframe_asset(name="my_df_asset")
            logging.info("Created new GX Asset.")

        try:
            batch_definition = asset.get_batch_definition("my_batch")
            logging.info(f"Found existing Batch Definition")
        except (LookupError, KeyError):
            batch_definition = asset.add_batch_definition_whole_dataframe("my_batch")
            logging.info(f"Created new Batch Definition")

        try:
            context.validation_definitions.delete("my_validation")
        except:
            pass
        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(
                name="my_validation",
                data=batch_definition,
                suite=suite,
            )
        )

        checkpoint_name = f"checkpoint_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=[validation_definition],
                result_format={"result_format": "COMPLETE"}
            )
        )
        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})
        results = list(checkpoint_result.run_results.values())[0]

        logging.warning("Result" + str(results))

        bad_indices = set()
        payload.is_schema_valid = True

        # Per-category tracking using sets of row indices
        completeness_indices = set()
        validity_indices = set()
        consistency_indices = set()
        type_indices = set()

        for r in results.results:
            exp_type = r.expectation_config.type
            category = EXPECTATION_CATEGORY_MAP.get(exp_type, 'other')

            if not r.success:
                indices = r.result.get('unexpected_index_list', [])
                bad_indices.update(indices)

                if category == 'completeness':
                    completeness_indices.update(indices)
                elif category == 'validity':
                    validity_indices.update(indices)
                elif category == 'consistency':
                    consistency_indices.update(indices)
                elif category == 'type':
                    type_indices.update(indices)
                elif category == 'schema':
                    payload.is_schema_valid = False
                    payload.schema_missing_column = list(set(COLUMNS) - set(df.columns.tolist()))
                    payload.schema_missing_column_count = len(payload.schema_missing_column)

            if exp_type == "expect_table_columns_to_match_set":
                payload.is_schema_valid = r.success
                if not r.success:
                    payload.schema_missing_column = list(set(COLUMNS) - set(df.columns.tolist()))
                    payload.schema_missing_column_count = len(payload.schema_missing_column)

        # Set category counts
        payload.missing_values_count = len(completeness_indices)
        payload.outlier_error_count = len(validity_indices)
        payload.consistency_error_count = len(consistency_indices)
        payload.type_error_count = len(type_indices)
        payload.schema_error_count = 0 if payload.is_schema_valid else 1

        payload.error_count = len(bad_indices)
        payload.error_rate = payload.error_count / payload.total_rows if payload.total_rows > 0 else 0

        if not payload.is_schema_valid:
            payload.error_count = payload.total_rows
            payload.error_rate = 1

        if not payload.is_schema_valid or payload.error_rate > 0.50:
            payload.error_criticality = "High"
        elif 0.10 <= payload.error_rate <= 0.50:
            payload.error_criticality = "Medium"
        elif 0 < payload.error_rate < 0.10:
            payload.error_criticality = "Low"
        else:
            payload.error_criticality = "None"

        logging.info(
            f"Category counts — completeness: {payload.missing_values_count}, "
            f"validity: {payload.outlier_error_count}, "
            f"consistency: {payload.consistency_error_count}, "
            f"type: {payload.type_error_count}, "
            f"schema: {payload.schema_error_count}"
        )

        # Convert back the cols
        if 'Date' in df.columns:
            df['Date'] = df['Date'].dt.strftime('%-m/%-d/%Y').fillna('invalid_date')
        df = df.astype(object).where(pd.notna(df), None)

        payload.is_processed = True
        bad_df = df.iloc[list(bad_indices)]
        good_df = df.drop(index=list(bad_indices))
        if payload.is_schema_valid:
            payload.bad_records = bad_df.to_dict(orient="records")
            payload.good_records = good_df.to_dict(orient="records")
        else:
            payload.bad_records = df.to_dict(orient="records")

        context.build_data_docs()
        data_docs_sites = context.get_docs_sites_urls()
        report_path = next(site["site_url"] for site in data_docs_sites if site["site_name"] == "local_site")
        payload.report_url = report_path

        return payload.model_dump()

    @task
    def save_statistics(data_to_ingest: dict) -> None:
        if not data_to_ingest:
            logging.info("No data received. Skipping database insert.")
            return

        file_name = data_to_ingest.get('source_filename', 'unknown_file')
        total_rows = data_to_ingest.get('total_rows', 0)
        error_count = data_to_ingest.get('error_count', 0)
        error_rate = data_to_ingest.get('error_rate', 0.0)
        is_schema_valid = data_to_ingest.get('is_schema_valid', True)
        error_criticality = data_to_ingest.get('error_criticality', 'None')
        missing_values_count = data_to_ingest.get('missing_values_count', 0)
        type_error_count = data_to_ingest.get('type_error_count', 0)
        outlier_error_count = data_to_ingest.get('outlier_error_count', 0)
        consistency_error_count = data_to_ingest.get('consistency_error_count', 0)
        schema_error_count = data_to_ingest.get('schema_error_count', 0)

        logging.info(f"Connecting to Postgres to save stats for {file_name}")

        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')

            insert_sql = """
                INSERT INTO data_quality_stats (
                    file_name, total_rows, error_count, error_rate, is_schema_valid, error_criticality,
                    missing_values_count, type_error_count, outlier_error_count,
                    consistency_error_count, schema_error_count,
                    total_rows_processed, total_clean_rows, total_corrupt_rows,
                    ingestion_timestamp
                ) VALUES (
                    %(file_name)s, %(total_rows)s, %(error_count)s, %(error_rate)s,
                    %(is_schema_valid)s, %(error_criticality)s,
                    %(missing_values_count)s, %(type_error_count)s, %(outlier_error_count)s,
                    %(consistency_error_count)s, %(schema_error_count)s,
                    %(total_rows)s, %(total_clean_rows)s, %(error_count)s,
                    CURRENT_TIMESTAMP
                );
            """

            params = {
                "file_name": file_name,
                "total_rows": total_rows,
                "error_count": error_count,
                "error_rate": error_rate,
                "is_schema_valid": is_schema_valid,
                "error_criticality": error_criticality,
                "missing_values_count": missing_values_count,
                "type_error_count": type_error_count,
                "outlier_error_count": outlier_error_count,
                "consistency_error_count": consistency_error_count,
                "schema_error_count": schema_error_count,
                "total_clean_rows": total_rows - error_count,
            }

            hook.run(insert_sql, parameters=params)
            logging.info(f"Successfully saved data quality stats for {file_name} to PostgreSQL.")

        except Exception as e:
            logging.error(f"Failed to save stats to database. Error: {str(e)}")
            raise

    @task
    def send_alerts(data_to_ingest: dict) -> None:
        if not data_to_ingest:
            logging.info("No data received. Skipping alert.")
            return {}

        try:
            logging.info("Starting Teams alert task")
            severity = data_to_ingest.get("error_criticality", "None")
            error_rate = data_to_ingest.get("error_rate", 0)
            total_rows = data_to_ingest.get("total_rows", 0)
            source_file = data_to_ingest.get("source_filename", "N/A")
            report_url = data_to_ingest.get("report_url", "N/A")
            is_schema_valid = data_to_ingest.get("is_schema_valid", False)

            error_percent = round(error_rate * 100, 2)

            if severity not in ["High", "Medium"]:
                logging.info(f"No alert sent (severity={severity})")
                return

            if severity == "High":
                icon = "🚨"
                color = "FF0000"
                title = "Data Quality Alert (Critical)"
            else:
                icon = "⚠️"
                color = "FFA500"
                title = "Data Quality Alert (Medium)"

            webhook_url = Variable.get("teams_webhook")

            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": title,
                "themeColor": color,
                "title": f"{icon} {title}",
                "sections": [
                    {
                        "facts": [
                            {"name": "Severity:", "value": severity},
                            {"name": "Schema Valid:", "value": is_schema_valid},
                            {"name": "Invalid Rows:", "value": f"{error_percent}%"},
                            {"name": "Total Rows:", "value": total_rows},
                            {"name": "Source File:", "value": source_file},
                            {"name": "Report:", "value": report_url}
                        ]
                    }
                ]
            }

            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()

            logging.info(
                f"Teams alert sent | severity={severity} | invalid_rows={error_percent}% | source_file={source_file} | report={report_url}"
            )

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send Teams alert: {e}")
            raise

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
            f"good_record_count:     {payload.total_rows - payload.error_count}\n"
            f"bad_record_count:      {payload.error_count}\n"
            f"missing_values_count:  {payload.missing_values_count}\n"
            f"type_error_count:      {payload.type_error_count}\n"
            f"outlier_error_count:   {payload.outlier_error_count}\n"
            f"consistency_errors:    {payload.consistency_error_count}\n"
            f"schema_error_count:    {payload.schema_error_count}\n"
        )

        if payload.good_records:
            os.makedirs(good_folder, exist_ok=True)
            good_filepath = f"{good_folder}{base_name}_{timestamp}.csv"
            df = pd.DataFrame(payload.good_records)
            df = df[COLUMNS]
            df.to_csv(good_filepath, index=False)
            logging.info(f"Successfully saved {len(payload.good_records)} rows to: {good_filepath}")
            summary_filepath = f"{good_folder}{base_name}_{timestamp}.txt"
            with open(summary_filepath, 'w') as f:
                f.write(summary)
        else:
            logging.info("No good records found to save.")

        if payload.bad_records:
            os.makedirs(bad_folder, exist_ok=True)
            bad_filepath = f"{bad_folder}{base_name}_{timestamp}.csv"
            df = pd.DataFrame(payload.bad_records)
            existing_cols = [cols for cols in COLUMNS if cols in df.columns]
            df = df[existing_cols]
            df.to_csv(bad_filepath, index=False)
            logging.warning(f"Saved {len(payload.bad_records)} rows with errors to: {bad_filepath}")
            summary_filepath = f"{bad_folder}{base_name}_{timestamp}.txt"
            with open(summary_filepath, 'w') as f:
                f.write(summary)
        else:
            logging.info("No bad records found. Data is 100% clean.")

    # Task relationships
    raw_data_to_ingest = read_data()
    file_content = validate_data(raw_data_to_ingest)
    save_statistics(file_content)
    send_alerts(file_content)
    split_and_save_data(file_content)

ingestion_validate_data()

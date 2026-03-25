from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging




# Define the DAG based exactly on the architecture diagram
with DAG(
    dag_id="data_ingestion_job",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *", # Matches the '5 min' loop in the diagram!
    catchup=False,
    tags=["ingestion", "data_quality"]
) as dag:

    # ==========================================
    # (1): Get Data 
    # ==========================================
    @task
    def get_data():
        """TODO: Code to pull the raw CSV files from the source."""
        print("Fetching raw data...")
        return {"file_path": "/opt/airflow/data/raw_data/batch_001.csv"}

    # ==========================================
    # (2) : Validate Data Quality
    # ==========================================
    @task
    def validate_data_quality(file_info: dict):
        """
        TODO: Implement Great Expectations here to validate the data.
        """
        # This should be the output manner, has mock data for now
        stats = {
            "file_name": "batch_001.csv",
            "total_rows": 500,
            "clean_rows": 200,
            "corrupt_rows": 150,
            "critical_issue_found": True # Flag to trigger Step 3
        }
        return stats

    # ==========================================
    # (3) : Send Alerts
    # ==========================================
    @task
    def send_alerts(stats: dict):
        """TODO: Send alerts to Teams/Slack if there are critical issues."""

    # ==========================================
    # (4) : Save Errors in DB
    # ==========================================
    @task
    def save_errors_in_db(stats: dict):
        if not stats:
            logging.info("No stats received. Skipping database insert.")
            return

        logging.info(f"Connecting to Postgres to save stats for {stats.get('file_name', 'unknown file')}")
        
        try:
            # Connecting to PostgreSQL using Airflow's secure hook
            hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # The SQL Insert
            insert_sql = """
                INSERT INTO data_quality_stats (
                    file_name, 
                    total_rows_processed, 
                    total_clean_rows,
                    total_corrupt_rows, 
                    missing_values_count, 
                    type_error_count, 
                    outlier_error_count
                ) VALUES (
                    %(file_name)s, 
                    %(total_rows_processed)s, 
                    %(total_clean_rows)s,
                    %(total_corrupt_rows)s, 
                    %(missing_values_count)s, 
                    %(type_error_count)s, 
                    %(outlier_error_count)s
                );
            """
            
            # Map the Python dictionary to the SQL command
            params = {
                "file_name": stats["file_name"],
                "total_rows_processed": stats["total_rows"],
                "total_clean_rows": stats["clean_rows"],
                "total_corrupt_rows": stats["corrupt_rows"],
                "missing_values_count": stats["missing_values"],
                "type_error_count": stats["type_errors"],
                "outlier_error_count": stats["outliers"]
            }
            
            # Upload to db
            hook.run(insert_sql, parameters=params)
            logging.info(f"Successfully saved data quality stats for {stats['file_name']} to PostgreSQL.")
            
        except Exception as e:
            # This will show up in bright red in the Airflow task logs!
            logging.error(f"Failed to save stats to database. Error: {str(e)}")
            # Raise the error again so Airflow accurately marks the task as failed
            raise


# This is the flow(Edit if needed)
    # Get the data
    raw_file = get_data()
    
    # validation func 
    validation_results = validate_data_quality(raw_file)
    
    # After validation send alerts if necessary and save to DB
    send_alerts(validation_results)
    save_errors_in_db(validation_results)
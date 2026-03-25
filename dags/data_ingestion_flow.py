from datetime import datetime
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
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
    def send_alerts(**context):
        try:
            webhook_url = Variable.get("teams_webhook")
            
            logging.info("Starting Teams alert task")
            # Pull validation result from previous task
            ti = context["ti"]
            validation_result = ti.xcom_pull(task_ids="validate_data")

            invalid_percent = validation_result["invalid_percent"]
            error_summary = validation_result["summary"]
            report_file = validation_result["report_file"]

            # Determine criticality
            if validation_result["missing_column"] or invalid_percent > 50:
                severity = "HIGH"
                icon = "🚨"
                title = "CRITICAL Data Quality Alert"
            elif 10 <= invalid_percent <= 50:
                severity = "MEDIUM"
                icon = "⚠️"
                title = "Data Quality Alert (Medium)"
            elif 0 < invalid_percent < 10:
                severity = "LOW"
            else:
                severity = "NONE"

            # Only send alerts for MEDIUM or HIGH
            if severity in ["MEDIUM", "HIGH"]:
                payload = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "summary": title,
                    "themeColor": "FF0000" if severity == "HIGH" else "FFA500",
                    "title": f"{icon} {title}",
                    "sections": [
                        {
                            "facts": [
                                {"name": "Severity:", "value": severity},
                                {"name": "Invalid Rows:", "value": f"{invalid_percent}%"},
                                {"name": "Report:", "value": report_file}
                            ]
                        },
                        {
                            "text": f"**Summary:** {error_summary}"
                        }
                    ]
                }

                requests.post(webhook_url, json=payload)
                logging.info(f"Teams alert sent successfully | report={report_file} | severity={severity} | invalid_rows={invalid_percent}%")
            else:
                logging.info("No alert needed (LOW or NONE)")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send Teams alert: {e}")
            raise

    # ==========================================
    # (4) : Save Errors in DB
    # ==========================================
    @task
    def save_errors_in_db(stats: dict):

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
        print(f" Saved data quality stats for {stats['file_name']} to PostgreSQL.")


# This is the flow(Edit if needed)
    # Get the data
    raw_file = get_data()
    
    # validation func 
    validation_results = validate_data_quality(raw_file)
    
    # After validation send alerts if necessary and save to DB
    send_alerts(validation_results)
    save_errors_in_db(validation_results)
"""
Airflow DAG for Data Drift Detection.

Reads CSV files from the drift_data directory, computes the same set of statistics
used in training_statistics (target, covariate, concept), and stores each batch
in the drift_statistics table for Grafana comparison.
"""

import os
import logging
import pandas as pd
from datetime import datetime

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

DRIFT_DATA_DIR = "/opt/airflow/data/drift_data/"


def _compute_stats(df: pd.DataFrame) -> dict:
    """Compute the full statistics payload from a dataframe."""
    n = len(df)

    def _source_mean(source):
        mask = df["Source"] == source
        return float(df.loc[mask, "Production"].mean()) if mask.any() else None

    return {
        "row_count": n,
        # Target drift
        "production_mean": float(df["Production"].mean()),
        "production_std": float(df["Production"].std()),
        "production_min": float(df["Production"].min()),
        "production_max": float(df["Production"].max()),
        "production_p25": float(df["Production"].quantile(0.25)),
        "production_p50": float(df["Production"].quantile(0.50)),
        "production_p75": float(df["Production"].quantile(0.75)),
        # Covariate drift — numeric
        "start_hour_mean": float(df["Start_Hour"].mean()),
        "start_hour_std": float(df["Start_Hour"].std()),
        "end_hour_mean": float(df["End_Hour"].mean()),
        "end_hour_std": float(df["End_Hour"].std()),
        "day_of_year_mean": float(df["Day_of_Year"].mean()),
        "day_of_year_std": float(df["Day_of_Year"].std()),
        # Covariate drift — categorical (%)
        "solar_percentage": float((df["Source"] == "Solar").sum() / n * 100),
        "wind_percentage": float((df["Source"] == "Wind").sum() / n * 100),
        "mixed_percentage": float((df["Source"] == "Mixed").sum() / n * 100),
        "spring_percentage": float((df["Season"] == "Spring").sum() / n * 100),
        "summer_percentage": float((df["Season"] == "Summer").sum() / n * 100),
        "fall_percentage": float((df["Season"] == "Fall").sum() / n * 100),
        "winter_percentage": float((df["Season"] == "Winter").sum() / n * 100),
        # Concept drift — conditional production mean by energy source
        "solar_production_mean": _source_mean("Solar"),
        "wind_production_mean": _source_mean("Wind"),
        "mixed_production_mean": _source_mean("Mixed"),
    }


INSERT_SQL = """
    INSERT INTO drift_statistics (
        check_date, source_file, row_count,
        production_mean, production_std, production_min, production_max,
        production_p25, production_p50, production_p75,
        start_hour_mean, start_hour_std,
        end_hour_mean, end_hour_std,
        day_of_year_mean, day_of_year_std,
        solar_percentage, wind_percentage, mixed_percentage,
        spring_percentage, summer_percentage, fall_percentage, winter_percentage,
        solar_production_mean, wind_production_mean, mixed_production_mean
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s
    );
"""


@dag(
    dag_id="drift_detection_v1",
    description="Compute and store drift statistics from incoming data batches",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["dsp", "mlops", "drift"],
)
def drift_detection_pipeline():

    @task
    def scan_drift_files() -> list:
        logger = logging.getLogger("airflow.task")
        os.makedirs(DRIFT_DATA_DIR, exist_ok=True)
        files = [f for f in os.listdir(DRIFT_DATA_DIR) if f.endswith(".csv")]
        if not files:
            raise AirflowSkipException("No CSV files found in drift_data directory.")
        logger.info(f"Found {len(files)} drift file(s): {files}")
        return files

    @task
    def compute_and_store_stats(filenames: list):
        logger = logging.getLogger("airflow.task")
        hook = PostgresHook(postgres_conn_id="postgres_default")

        for filename in filenames:
            filepath = os.path.join(DRIFT_DATA_DIR, filename)
            try:
                df = pd.read_csv(filepath)
                if df.empty:
                    logger.warning(f"Skipping empty file: {filename}")
                    continue

                stats = _compute_stats(df)
                hook.run(INSERT_SQL, parameters=(
                    datetime.now(), filename, stats["row_count"],
                    stats["production_mean"], stats["production_std"],
                    stats["production_min"], stats["production_max"],
                    stats["production_p25"], stats["production_p50"], stats["production_p75"],
                    stats["start_hour_mean"], stats["start_hour_std"],
                    stats["end_hour_mean"], stats["end_hour_std"],
                    stats["day_of_year_mean"], stats["day_of_year_std"],
                    stats["solar_percentage"], stats["wind_percentage"], stats["mixed_percentage"],
                    stats["spring_percentage"], stats["summer_percentage"],
                    stats["fall_percentage"], stats["winter_percentage"],
                    stats["solar_production_mean"], stats["wind_production_mean"],
                    stats["mixed_production_mean"],
                ))
                logger.info(f"Stored drift stats for '{filename}' ({stats['row_count']} rows).")
            except Exception as e:
                logger.error(f"Failed to process '{filename}': {e}")
                raise

    files = scan_drift_files()
    compute_and_store_stats(files)


drift_detection_job = drift_detection_pipeline()

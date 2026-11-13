"""
Airflow DAG for Data Drift Detection.

Aggregates all new CSV files from good_data (newer than the last processed
ingestion timestamp) into one batch, computes target/covariate/concept drift
statistics, and stores a single record in drift_statistics for Grafana comparison.

A batch is eligible when it contains at least 800 rows in total.
"""

import os
import re
import logging
import pandas as pd
from datetime import datetime
from typing import Optional

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

GOOD_DATA_DIR = "/opt/airflow/data/good_data/"
MIN_ROWS_FOR_DRIFT = 800
WATERMARK_VAR = "drift_last_ingestion_ts"

_FILENAME_TS_RE = re.compile(r"_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv$")


def _parse_file_timestamp(filename: str) -> Optional[datetime]:
    match = _FILENAME_TS_RE.search(filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


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
    def scan_good_files() -> dict:
        logger = logging.getLogger("airflow.task")

        # Determine cutoff timestamp
        watermark = Variable.get(WATERMARK_VAR, default_var=None)
        if watermark:
            cutoff = datetime.strptime(watermark, "%Y-%m-%d_%H-%M-%S")
            logger.info(f"Using watermark as cutoff: {cutoff}")
        else:
            hook = PostgresHook(postgres_conn_id="postgres_default")
            row = hook.get_first("SELECT MAX(training_date) FROM training_statistics;")
            training_date = row[0] if row and row[0] else None
            if training_date is None:
                raise AirflowSkipException("No watermark and no training baseline found. Run training first.")
            if hasattr(training_date, "tzinfo") and training_date.tzinfo is not None:
                training_date = training_date.replace(tzinfo=None)
            cutoff = training_date
            logger.info(f"No watermark found. Using training_date as cutoff: {cutoff}")

        os.makedirs(GOOD_DATA_DIR, exist_ok=True)
        candidates = []
        max_ts = None

        for filename in os.listdir(GOOD_DATA_DIR):
            if not filename.endswith(".csv"):
                continue
            file_ts = _parse_file_timestamp(filename)
            if file_ts is None:
                logger.warning(f"Cannot parse timestamp from '{filename}', skipping.")
                continue
            if file_ts <= cutoff:
                continue
            candidates.append(filename)
            if max_ts is None or file_ts > max_ts:
                max_ts = file_ts

        if not candidates:
            raise AirflowSkipException("No new files found in good_data directory.")

        logger.info(f"Found {len(candidates)} candidate file(s): {candidates}")
        return {
            "files": candidates,
            "max_ts": max_ts.strftime("%Y-%m-%d_%H-%M-%S"),
        }

    @task
    def compute_and_store_stats(scan_result: dict):
        logger = logging.getLogger("airflow.task")
        filenames = scan_result["files"]

        df_list = []
        for filename in filenames:
            filepath = os.path.join(GOOD_DATA_DIR, filename)
            df_list.append(pd.read_csv(filepath))

        combined_df = pd.concat(df_list, ignore_index=True)
        total_rows = len(combined_df)

        if total_rows < MIN_ROWS_FOR_DRIFT:
            raise AirflowSkipException(
                f"Only {total_rows} rows across {len(filenames)} file(s). "
                f"Minimum {MIN_ROWS_FOR_DRIFT} required."
            )

        logger.info(f"Aggregated {total_rows} rows from {len(filenames)} file(s).")

        stats = _compute_stats(combined_df)
        batch_label = f"batch_{scan_result['max_ts']}"

        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(INSERT_SQL, parameters=(
            datetime.now(), batch_label, stats["row_count"],
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
        logger.info(f"Stored drift stats for '{batch_label}' ({total_rows} rows).")

    @task
    def update_watermark(scan_result: dict):
        Variable.set(WATERMARK_VAR, scan_result["max_ts"])
        logger = logging.getLogger("airflow.task")
        logger.info(f"Updated watermark to {scan_result['max_ts']}")

    scan_result = scan_good_files()
    compute_and_store_stats(scan_result) >> update_watermark(scan_result)


drift_detection_job = drift_detection_pipeline()

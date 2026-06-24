"""
Airflow DAG for Automated Machine Learning Training and Promotion.
"""

import os
import shutil
import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import mean_squared_log_error

# --- Constants & Config ---
GOOD_DATA_DIR = "/opt/airflow/data/good_data/"
ARCHIVED_DATA_DIR = "/opt/airflow/data/archived_data/"
TEMP_DATA_DIR = "/tmp/airflow_temp/"
MIN_ROWS_FOR_TRAINING = 500
MODEL_NAME = "RenewableEnergyModel"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

mlflow.set_tracking_uri(MLFLOW_URI)


@dag(
    dag_id="training_job_v1",
    description="Train, evaluate, and promote ML models via MLflow",
    schedule="0 2 * * 0",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["dsp", "mlops", "training"]
)
def ml_training_pipeline():

    @task
    def load_data() -> dict:
        logger = logging.getLogger("airflow.task")
        os.makedirs(TEMP_DATA_DIR, exist_ok=True)

        csv_files = [f for f in os.listdir(GOOD_DATA_DIR) if f.endswith('.csv')]

        if not csv_files:
            raise AirflowSkipException("No new data found in good_data directory. Skipping training.")

        df_list = []
        files_to_archive = []
        for file in csv_files:
            filepath = os.path.join(GOOD_DATA_DIR, file)
            df_list.append(pd.read_csv(filepath))
            files_to_archive.append(file)

        combined_df = pd.concat(df_list, ignore_index=True)
        total_rows = len(combined_df)

        if total_rows < MIN_ROWS_FOR_TRAINING:
            raise AirflowSkipException(f"Only {total_rows} rows available. Require {MIN_ROWS_FOR_TRAINING}.")

        logger.info(f"Aggregated {total_rows} rows from {len(csv_files)} files.")

        temp_filepath = os.path.join(TEMP_DATA_DIR, "current_training_data.csv")
        combined_df.to_csv(temp_filepath, index=False)

        return {"temp_data_path": temp_filepath, "processed_files": files_to_archive}

    @task
    def train_model(data_info: dict) -> dict:
        logger = logging.getLogger("airflow.task")
        df = pd.read_csv(data_info["temp_data_path"])

        # 1. Define Features (Exact match to train_evaluate.py)
        features = ['Start_Hour', 'End_Hour', 'Source', 'Month_Name', 'Season', 'Day_of_Year', 'Day_Name']
        X = df[features]
        y = df['Production']

        # 2. Split Data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # 3. Build Preprocessor
        categorical_cols = ['Source', 'Month_Name', 'Season', 'Day_Name']
        preprocessor = ColumnTransformer(
            transformers=[('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)],
            remainder='passthrough'
        )

        # 4. Define Feature Selector and Model
        feature_selector = SelectFromModel(
            estimator=RandomForestRegressor(n_estimators=30, max_depth=10, random_state=42),
            threshold="median"
        )

        final_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=12,
            min_samples_leaf=2,
            random_state=42
        )

        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('feature_selection', feature_selector),
            ('regressor', final_model)
        ])

        # 5. Train and Evaluate (Logged to MLflow)
        mlflow.set_experiment("Energy_Production_Forecasting")
        with mlflow.start_run() as run:
            logger.info("Training Random Forest model (100 trees)...")
            pipeline.fit(X_train, y_train)

            preds = pipeline.predict(X_test)
            preds = np.clip(preds, 0, None)

            rmsle = np.sqrt(mean_squared_log_error(y_test, preds))
            logger.info(f"Final Model RMSLE: {rmsle:.4f}")

            mlflow.log_metric("rmsle", rmsle)
            mlflow.log_params({"n_estimators": 100, "max_depth": 12, "min_samples_leaf": 2})

            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="model",
                registered_model_name=MODEL_NAME
            )

            run_id = run.info.run_id

        return {
            "run_id": run_id,
            "candidate_rmsle": rmsle,
            "temp_data_path": data_info["temp_data_path"],
            "processed_files": data_info["processed_files"]
        }

    @task
    def save_training_stats(train_info: dict) -> dict:
        logger = logging.getLogger("airflow.task")
        df = pd.read_csv(train_info["temp_data_path"])

        production_mean = float(df['Production'].mean())
        production_std = float(df['Production'].std())
        solar_percentage = float((df['Source'] == 'Solar').mean() * 100)

        hook = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = """
            INSERT INTO training_statistics (
                run_id, training_date, production_mean, production_std, solar_percentage
            ) VALUES (%s, %s, %s, %s, %s);
        """
        try:
            hook.run(insert_sql, parameters=(
                train_info["run_id"], datetime.now(), production_mean, production_std, solar_percentage
            ))
            logger.info("Saved training baseline statistics to DB.")
        except Exception as e:
            logger.warning(f"Could not save stats. Error: {e}")

        return train_info

    @task
    def evaluate_candidate(train_info: dict) -> dict:
        logger = logging.getLogger("airflow.task")
        client = MlflowClient()

        # 1. Cast NumPy float to standard Python float
        candidate_rmsle = float(train_info["candidate_rmsle"])

        try:
            champion_meta = client.get_model_version_by_alias(name=MODEL_NAME, alias="champion")
            champion_run = mlflow.get_run(champion_meta.run_id)
            # 2. Use 9999.0 instead of inf to keep JSON safe
            champion_rmsle = float(champion_run.data.metrics.get("rmsle", 9999.0))
            logger.info(f"Found existing Champion. RMSLE: {champion_rmsle:.4f}")
        except Exception:
            logger.info("No @champion model found in MLflow. Automatic promotion granted.")
            champion_rmsle = 9999.0

        # 3. Cast NumPy bool to standard Python bool
        should_promote = bool(candidate_rmsle <= champion_rmsle)

        return {
            "run_id": train_info["run_id"],
            "should_promote": should_promote,
            "candidate_rmsle": candidate_rmsle,
            "champion_rmsle": champion_rmsle,
            "processed_files": train_info["processed_files"]
        }

    @task
    def promote_to_champion(eval_info: dict) -> dict:
        logger = logging.getLogger("airflow.task")
        client = MlflowClient()

        if not eval_info["should_promote"]:
            logger.warning("Candidate failed to beat Champion. Skipping promotion.")

            # Safe Variable get for Airflow 3 SDK
            try:
                webhook_url = Variable.get("teams_webhook")
            except Exception:
                webhook_url = None

            if webhook_url:
                payload = {
                    "@type": "MessageCard",
                    "themeColor": "FF0000",
                    "title": "ML Model Promotion Failed",
                    "text": (
                        f"Candidate RMSLE ({eval_info['candidate_rmsle']:.4f}) "
                        f"was worse than Champion ({eval_info['champion_rmsle']:.4f})."
                    ),
                }
                requests.post(webhook_url, json=payload)

            raise AirflowSkipException("Candidate model rejected.")

        logger.info("Candidate beat Champion! Promoting to @champion alias.")
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        target_version = next(v for v in versions if v.run_id == eval_info["run_id"])

        client.set_registered_model_alias(name=MODEL_NAME, alias="champion", version=target_version.version)
        return eval_info

    @task
    def notify_api_reload(promotion_info: dict):
        logger = logging.getLogger("airflow.task")
        try:
            response = requests.post("http://fastapi:80/reload-model", timeout=10)
            response.raise_for_status()
            logger.info("Notified API to reload model.")
        except Exception as e:
            logger.error(f"Failed to reload API: {e}")
            raise

    @task
    def archive_data(promotion_info: dict):
        os.makedirs(ARCHIVED_DATA_DIR, exist_ok=True)

        for filename in promotion_info["processed_files"]:
            src = os.path.join(GOOD_DATA_DIR, filename)
            dst = os.path.join(ARCHIVED_DATA_DIR, filename)
            if os.path.exists(src):
                shutil.move(src, dst)

    # --- Pipeline Flow ---
    data = load_data()
    train_res = train_model(data)
    stats_res = save_training_stats(train_res)
    eval_res = evaluate_candidate(stats_res)
    promotion_res = promote_to_champion(eval_res)

    promotion_res >> [notify_api_reload(promotion_res), archive_data(promotion_res)]


training_job = ml_training_pipeline()

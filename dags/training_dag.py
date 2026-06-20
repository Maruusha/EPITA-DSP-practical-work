"""
Airflow DAG for Automated Machine Learning Training and Promotion.

This pipeline automates the ML lifecycle:
1. Loads validated data from the `good_data` directory.
2. Trains a new RandomForestRegressor and logs metrics to MLflow.
3. Computes and saves training data statistics to PostgreSQL for drift monitoring.
4. Evaluates the candidate against the current @champion model.
5. Promotes the candidate if it outperforms the champion.
6. Notifies the FastAPI service to reload the model and archives used data.
"""

import os
import time
import shutil
import logging
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
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
TEMP_DATA_DIR = "/opt/airflow/data/temp/"
MIN_ROWS_FOR_TRAINING = 500  # Minimum new rows required to trigger a training run
MODEL_NAME = "RenewableEnergyModel"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Set MLflow tracking URI
mlflow.set_tracking_uri(MLFLOW_URI)


@dag(
    dag_id="training_job_v1",
    description="Train, evaluate, and promote ML models via MLflow",
    schedule_interval="0 2 * * 0",  # Runs weekly (Sunday at 2:00 AM) - adjust as needed
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["dsp", "mlops", "training"]
)
def ml_training_pipeline():

    @task
    def load_data() -> dict:
        """
        Scans good_data for new files. If sufficient data exists, combines it
        into a temporary training dataset and returns the file paths.
        """
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
            raise AirflowSkipException(f"Only {total_rows} rows available. Require {MIN_ROWS_FOR_TRAINING}. Skipping.")

        logger.info(f"Aggregated {total_rows} rows from {len(csv_files)} files.")
        
        # Save to temp file for downstream tasks to consume (avoids XCom size limits)
        temp_filepath = os.path.join(TEMP_DATA_DIR, "current_training_data.csv")
        combined_df.to_csv(temp_filepath, index=False)
        
        return {
            "temp_data_path": temp_filepath,
            "processed_files": files_to_archive
        }

    @task
    def train_model(data_info: dict) -> dict:
        """
        Trains the Random Forest model and logs it to MLflow.
        """
        logger = logging.getLogger("airflow.task")
        df = pd.read_csv(data_info["temp_data_path"])
        
        # Prepare Features
        features = ['Start_Hour', 'End_Hour', 'Source', 'Month_Name', 'Season', 'Day_of_Year', 'Day_Name']
        X = df[features]
        y = df['Production']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Build Pipeline matching baseline logic
        categorical_cols = ['Source', 'Month_Name', 'Season', 'Day_Name']
        preprocessor = ColumnTransformer(
            transformers=[('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)],
            remainder='passthrough'
        )

        feature_selector = SelectFromModel(
            estimator=RandomForestRegressor(n_estimators=30, max_depth=10, random_state=42),
            threshold="median"
        )

        final_model = RandomForestRegressor(n_estimators=100, max_depth=12, min_samples_leaf=2, random_state=42)

        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('feature_selection', feature_selector),
            ('regressor', final_model)
        ])

        # MLflow Tracking
        mlflow.set_experiment("Energy_Production_Forecasting")
        with mlflow.start_run() as run:
            logger.info("Training candidate model...")
            pipeline.fit(X_train, y_train)
            
            # Predict & Calculate Metric
            preds = pipeline.predict(X_test)
            preds = np.clip(preds, 0, None)
            rmsle = np.sqrt(mean_squared_log_error(y_test, preds))
            
            logger.info(f"Candidate trained. RMSLE: {rmsle:.4f}")

            # Log to MLflow
            mlflow.log_metric("rmsle", rmsle)
            mlflow.log_params({"n_estimators": 100, "max_depth": 12, "min_samples_leaf": 2})
            
            # Register the model
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
        """
        Calculates feature distribution statistics and saves them to PostgreSQL
        to act as a baseline for Grafana drift monitoring.
        """
        logger = logging.getLogger("airflow.task")
        df = pd.read_csv(train_info["temp_data_path"])
        
        # Example calculations for drift monitoring
        production_mean = float(df['Production'].mean())
        production_std = float(df['Production'].std())
        solar_percentage = float((df['Source'] == 'Solar').mean() * 100)
        
        # Push to PostgreSQL
        hook = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = """
            INSERT INTO training_statistics (
                run_id, training_date, production_mean, production_std, solar_percentage
            ) VALUES (%s, %s, %s, %s, %s);
        """
        try:
            # Note: You will need to create this table in your db_utility.py
            hook.run(insert_sql, parameters=(
                train_info["run_id"], datetime.now(), production_mean, production_std, solar_percentage
            ))
            logger.info("Successfully saved training baseline statistics to DB.")
        except Exception as e:
            logger.warning(f"Could not save stats (table may not exist yet). Error: {e}")

        return train_info

    @task
    def evaluate_candidate(train_info: dict) -> dict:
        """
        Compares the newly trained candidate against the current @champion.
        Promotes if Candidate RMSLE <= Champion RMSLE.
        """
        logger = logging.getLogger("airflow.task")
        client = MlflowClient()
        candidate_rmsle = train_info["candidate_rmsle"]
        run_id = train_info["run_id"]

        try:
            # Fetch the current champion version
            champion_meta = client.get_model_version_by_alias(name=MODEL_NAME, alias="champion")
            champion_run = mlflow.get_run(champion_meta.run_id)
            champion_rmsle = champion_run.data.metrics.get("rmsle", float('inf'))
            logger.info(f"Found existing Champion. RMSLE: {champion_rmsle:.4f}")
        except Exception:
            # The "Cold Start" Problem: No champion exists yet
            logger.info("No @champion model found in MLflow. Automatic promotion granted.")
            champion_rmsle = float('inf')

        # Criteria: Lower RMSLE is better. Candidate must match or exceed (be lower).
        should_promote = candidate_rmsle <= champion_rmsle

        return {
            "run_id": run_id,
            "should_promote": should_promote,
            "candidate_rmsle": candidate_rmsle,
            "champion_rmsle": champion_rmsle,
            "processed_files": train_info["processed_files"]
        }

    @task
    def promote_to_champion(eval_info: dict) -> dict:
        """
        Assigns the @champion alias in MLflow if evaluation passed.
        If it failed, sends a Teams alert.
        """
        logger = logging.getLogger("airflow.task")
        client = MlflowClient()

        if not eval_info["should_promote"]:
            logger.warning("Candidate failed to beat Champion. Skipping promotion.")
            
            # Send Teams Alert for failed promotion
            webhook_url = Variable.get("teams_webhook", default_var=None)
            if webhook_url:
                payload = {
                    "@type": "MessageCard",
                    "themeColor": "FF0000",
                    "title": "❌ ML Model Promotion Failed",
                    "text": f"Candidate RMSLE ({eval_info['candidate_rmsle']:.4f}) was worse than Champion ({eval_info['champion_rmsle']:.4f})."
                }
                requests.post(webhook_url, json=payload)
                
            raise AirflowSkipException("Candidate model rejected.")

        logger.info("Candidate beat Champion! Promoting to @champion alias.")
        
        # Find the specific version number for this run_id
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        target_version = next(v for v in versions if v.run_id == eval_info["run_id"])
        
        # Set the alias (This automatically removes it from the previous champion)
        client.set_registered_model_alias(
            name=MODEL_NAME, 
            alias="champion", 
            version=target_version.version
        )

        return eval_info

    @task
    def notify_api_reload(promotion_info: dict):
        """
        Pings the FastAPI service to pull the new model from MLflow.
        """
        logger = logging.getLogger("airflow.task")
        api_url = "http://fastapi:80/reload-model"
        
        try:
            response = requests.post(api_url, timeout=10)
            response.raise_for_status()
            logger.info("Successfully notified API to reload model.")
        except Exception as e:
            logger.error(f"Failed to reload API: {e}")
            raise

    @task
    def archive_data(promotion_info: dict):
        """
        Moves the used CSV files out of good_data to prevent retraining on them.
        """
        logger = logging.getLogger("airflow.task")
        os.makedirs(ARCHIVED_DATA_DIR, exist_ok=True)
        
        count = 0
        for filename in promotion_info["processed_files"]:
            src = os.path.join(GOOD_DATA_DIR, filename)
            dst = os.path.join(ARCHIVED_DATA_DIR, filename)
            if os.path.exists(src):
                shutil.move(src, dst)
                count += 1
                
        logger.info(f"Successfully archived {count} files.")

    # --- Define Task Dependencies (The Pipeline Flow) ---
    data = load_data()
    train_res = train_model(data)
    stats_res = save_training_stats(train_res)
    eval_res = evaluate_candidate(stats_res)
    promotion_res = promote_to_champion(eval_res)
    
    # Parallel execution of API reload and Archiving
    promotion_res >> [notify_api_reload(promotion_res), archive_data(promotion_res)]

training_job = ml_training_pipeline()
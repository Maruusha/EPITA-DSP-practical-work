import random
import pickle
import os
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
import mlflow
import mlflow.pyfunc

USE_TEMP_MODEL = False

class EnergyModel(ABC):
    version: str
    @abstractmethod
    def predict(self, features: dict) -> float: ...

class TempModel(EnergyModel):
    def __init__(self):
        self.version = "Mock-v0.1"
    def predict(self, features: dict) -> float:
        return round(random.uniform(1000.0, 5000.0), 1)
        
class RealModel(EnergyModel):
    def __init__(self):
        # Consistent Version Naming for Local Fallback
        self.version = "Local-Baseline-v3.0"
        
        model_paths = ["baseline_model.pkl", "model_service/baseline_model.pkl"]
        self.model = None
        
        for path in model_paths:
            if os.path.exists(path):
                with open(path, "rb") as f:
                    self.model = pickle.load(f)
                break
                
        if self.model is None:
            raise FileNotFoundError("Could not locate baseline_model.pkl!")

    def predict(self, features: dict) -> float:
        date_obj = features['date']
        
        # Calculate derived temporal features
        day_of_year = date_obj.timetuple().tm_yday
        month_name = date_obj.strftime("%B")
        day_name = date_obj.strftime("%A") 
        
        # Determine the Season
        m = date_obj.month
        if m in [12, 1, 2]:
            season = "Winter"
        elif m in [3, 4, 5]:
            season = "Spring"
        elif m in [6, 7, 8]:
            season = "Summer"
        else:
            season = "Fall"
            
        # Build the dataframe with all 7 features for the Random Forest
        input_df = pd.DataFrame([{
            'Start_Hour': features['start_hour'],
            'End_Hour': features['end_hour'],
            'Source': features['energy_source'],
            'Month_Name': month_name,
            'Season': season,
            'Day_of_Year': day_of_year,
            'Day_Name': day_name
        }])
        
        prediction = self.model.predict(input_df)[0]
        
        if prediction < 0:
            prediction = 0.0
            
        return round(float(prediction), 1)

class MLflowChampionModel(EnergyModel):
    """
    Loads the promoted @champion model directly from the MLflow Model Registry.
    """
    def __init__(self):
        mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(mlflow_uri)
        
        model_uri = "models:/RenewableEnergyModel@champion"
        print(f"Attempting to load MLflow model from {model_uri}...")
        
        self.model = mlflow.pyfunc.load_model(model_uri)
        
        # Consistent Version Naming for Production
        # Prefixes 'MLflow-Prod-' and grabs the short 8-character hash of the run_id.
        # This keeps database rows clean while staying completely traceable.
        raw_run_id = self.model.metadata.run_id
        self.version = f"MLflow-Prod-{raw_run_id[:8]}"

    def predict(self, features: dict) -> float:
        date_obj = features['date']
        
        day_of_year = date_obj.timetuple().tm_yday
        month_name = date_obj.strftime("%B")
        day_name = date_obj.strftime("%A")
        
        m = date_obj.month
        if m in [12, 1, 2]:
            season = "Winter"
        elif m in [3, 4, 5]:
            season = "Spring"
        elif m in [6, 7, 8]:
            season = "Summer"
        else:
            season = "Fall"
            
        input_df = pd.DataFrame([{
            'Start_Hour': features['start_hour'],
            'End_Hour': features['end_hour'],
            'Source': features['energy_source'],
            'Month_Name': month_name,
            'Season': season,
            'Day_of_Year': day_of_year,
            'Day_Name': day_name
        }])
        
        prediction = self.model.predict(input_df)[0]
        
        if prediction < 0:
            prediction = 0.0
            
        return round(float(prediction), 1)

def load_model():
    """
    Attempts to load the MLflow Champion. Falls back to the local baseline 
    if MLflow is empty (e.g., before the first Airflow training run completes).
    """
    if USE_TEMP_MODEL:
        return TempModel()
        
    try:
        return MLflowChampionModel()
    except Exception as e:
        print(f"MLflow champion not found. Falling back to local .pkl. Reason: {e}")
        return RealModel()
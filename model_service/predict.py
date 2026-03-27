import random
import pickle
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
import os

USE_TEMP_MODEL = True

class EnergyModel(ABC):
    version: str
    @abstractmethod
    def predict(self, features: dict) -> float: ...

class TempModel(EnergyModel):
    def __init__(self):
        self.version = "0.1"
    def predict(self, features: dict) -> float:
        return round(random.uniform(1000.0, 5000.0), 1)
        
class RealModel(EnergyModel):
    def __init__(self):
        self.version = "3.0-RandomForest"
        
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
        # Extract the date object
        date_obj = features['date']
        
        # Calculate derived temporal features
        day_of_year = date_obj.timetuple().tm_yday
        month_name = date_obj.strftime("%B")
        day_name = date_obj.strftime("%A") # Extracts "Monday", "Tuesday", etc.
        
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
        
        # Safety net
        if prediction < 0:
            prediction = 0.0
            
        return round(float(prediction), 1)

def load_model():
    if USE_TEMP_MODEL:
        return TempModel()
    return RealModel()
import random
from datetime import datetime
from abc import ABC, abstractmethod


USE_TEMP_MODEL = True

class EnergyModel(ABC):
    version: str

    @abstractmethod
    def predict(self, features: dict) -> float: ...


class TempModel(EnergyModel):

    def __init__(self):
        self.version = "0.1"

    def predict(self, features: dict) -> float:
        prediction = round(random.uniform(1000.0, 5000.0), 1)
        return prediction
        
class RealModel(EnergyModel)   :
    # TODO
    def __init__(self):
        self.version = "1.0"

    def predict(self, features: dict) -> float:
        raise NotImplementedError("RealModel.predict() is not yet implemented")

def load_model():
    if USE_TEMP_MODEL:
        return TempModel()
    return RealModel()


    


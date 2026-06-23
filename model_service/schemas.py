from datetime import date
from typing import List

from pydantic import BaseModel, field_validator, model_validator


class PredictionInput(BaseModel):
    date: date
    start_hour: int
    end_hour: int
    energy_source: str
    prediction_source: str

    @field_validator("energy_source")
    def validate_energy_source(cls, v):
        if not v.strip():
            raise ValueError("energy_source cannot be empty")
        return v

    @field_validator("prediction_source")
    def validate_prediction_source(cls, v):
        if not v.strip():
            raise ValueError("prediction_source cannot be empty")
        return v

    @field_validator("start_hour", "end_hour")
    def validate_hour_range(cls, v):
        if not 0 <= v <= 23:
            raise ValueError("must be between 0 and 23")
        return v

    @model_validator(mode="after")
    def validate_hour_order(self):
        if self.start_hour >= self.end_hour:
            raise ValueError("start_hour must be smaller than end_hour")
        return self


class Prediction(BaseModel):
    production: float
    model_version: str
    received_input: PredictionInput


class PredictionResponse(BaseModel):
    status: str = "success"
    predictions: List[Prediction]

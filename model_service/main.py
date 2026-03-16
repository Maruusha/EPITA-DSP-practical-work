import sys
from datetime import datetime
from typing import List, Optional, Union, Dict
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import predict
import db_utility

app = FastAPI(title="Energy Production Prediction Service")


# --- Pydantic Models ---
class PredictionInput(BaseModel):
    """Matches the exact structure sent by Streamlit."""
    date: str           # Expected format: "YYYY-MM-DD"
    start_hour: int
    end_hour: int
    energy_source: str  # e.g., "Wind", "Solar"
    prediction_source: str = "webapp"


class PredictionResult(BaseModel):
    production: float
    model_version: str
    received_input: Dict


class PredictionResponse(BaseModel):
    status: str
    predictions: List[PredictionResult]


def validate_results(results: list) -> None:
    """Raises if no valid predictions were returned."""
    if not results:
        raise ValueError("No valid predictions were made. Check if energy_source exists in DB.")


def format_prediction_response(results: list) -> dict:
    """Formats prediction results for the API response."""
    return {
        "status": "success",
        "predictions": [
            {
                "production": r["prediction"],
                "model_version": r["model_version"],
                "received_input": r["received_input"],
            }
            for r in results
        ]
    }


def check_db_connection() -> dict:
    """Validates DB connection and returns appropriate response."""
    if db_utility.test_db_connection():
        return {"status": "success", "message": "Connected to PostgreSQL!"}
    raise HTTPException(status_code=500, detail="Database connection failed")


def get_prediction_results(payload: List[PredictionInput]) -> dict:
    """Runs prediction pipeline and returns formatted response."""
    results = predict.process_and_log(payload)
    validate_results(results)
    return format_prediction_response(results)


# --- Endpoints ---
@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/test-db", tags=["System Checks"])
async def test_db():
    """Verifies that the API can talk to the Postgres container."""
    return check_db_connection()


@app.post("/predict",  response_model=PredictionResponse)
async def do_predict(payload: Union[PredictionInput, List[PredictionInput]]):
    try:
        input_list = payload if isinstance(payload, list) else [payload]
        return get_prediction_results(input_list)
    except Exception as e:
        print(f"Prediction Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/past-predictions")
async def get_history(
    ml_model: Optional[str] = None,
    energy_source_id: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Queries the database for historical prediction records."""
    return db_utility.query_predictions(
        ml_model=ml_model,
        energy_source_id=energy_source_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )
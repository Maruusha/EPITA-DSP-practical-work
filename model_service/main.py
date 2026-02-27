import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# Internal imports
import predict
import db_utility

app = FastAPI(title="Energy Production Prediction Service")

# --- Pydantic Models for Input Validation ---

class PredictionInput(BaseModel):
    """Matches the exact structure sent by Streamlit."""
    date: str           # Expected format: "YYYY-MM-DD"
    start_hour: int
    end_hour: int
    energy_source: str  # e.g., "Wind", "Solar"

class PredictionRequest(BaseModel):
    """Wrapper to allow batching or single object requests."""
    data: Union[PredictionInput, List[PredictionInput]]

# --- Lifecycle Events ---

@app.on_event("startup")
async def startup_event():
    """
    #Runs once when the container starts. 
    #Creates the tables in Postgres if they don't exist.
    """
    #print("Initializing database tables...")
    #db_utility.Base.metadata.create_all(bind=db_utility.engine, checkfirst=True)

# --- Endpoints ---

@app.get("/health")
async def health():   
    return {"status": "healthy"}

@app.get("/test-db", tags=["System Checks"])
async def test_db():
    """Verifies that the API can talk to the Postgres container."""
    if db_utility.test_db_connection():
        return {"status": "success", "message": "Connected to PostgreSQL!"}
    raise HTTPException(status_code=500, detail="Database connection failed")

@app.post("/predict")
async def do_predict(payload: PredictionRequest):
    """
    Main endpoint for Streamlit. 
    Processes input, gets model prediction, logs to DB, and returns results.
    """
    try:
        # 1. Normalize input to a list of objects
        input_list = payload.data if isinstance(payload.data, list) else [payload.data]
        
        # 2. Process logic (Prediction + DB Logging)
        # Returns a list of dicts: [{"prediction": float, "model_used": str, "input": dict}, ...]
        results = predict.process_and_log(input_list)
        
        if not results:
            raise ValueError("No valid predictions were made. Check if energy_source exists in DB.")

        # 3. Construct response for Streamlit (taking the first result for single calls)
        return {
            "prediction": str(results[0]["prediction"]),
            "model_version": results[0]["model_version"],
            "received_input": results[0]["received_input"],
            "status": "success"
        }
    except Exception as e:
        # Logs the error to Docker console and returns 500 to user
        print(f"Prediction Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/past-predictions")
async def get_history(
    ml_model: Optional[str] = None,
    input_source_id: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Queries the database for historical prediction records."""
    return db_utility.query_predictions(
        ml_model=ml_model,
        input_source_id=input_source_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )
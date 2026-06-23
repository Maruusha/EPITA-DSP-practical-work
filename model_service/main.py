from datetime import date, datetime, timezone
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Request, Depends
from contextlib import asynccontextmanager
from pydantic import BaseModel
from sqlalchemy.orm import Session
import predict
import db_utility
from schemas import PredictionInput, Prediction, PredictionResponse


def derive_temporal_features(d: date) -> dict:
    """Derive the temporal features the model pipeline uses, matching the training CSV columns."""
    month = d.month
    if month in (3, 4, 5):
        season = "Spring"
    elif month in (6, 7, 8):
        season = "Summer"
    elif month in (9, 10, 11):
        season = "Fall"
    else:
        season = "Winter"

    return {
        "day_of_year": d.timetuple().tm_yday,
        "day_name": d.strftime("%A"),    # "Monday", "Tuesday", ...
        "month_name": d.strftime("%B"),  # "January", "February", ...
        "season": season,
    }


# Set the max number of input for each batch request
MAX_BATCH_SIZE = 100


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load model on startup using lifespan
    app.state.model = predict.load_model()

    # Query energy_source table and use it for reference later
    db = next(db_utility.get_db())
    try:
        records = db_utility.get_all_energy_sources(db)
        app.state.energy_source_map = {row.source_type: row.id for row in records}
    finally:
        db.close()
    yield


# Initialize app using the lifespan
app = FastAPI(
    title="Renewable Energy Production Prediction Service",
    lifespan=lifespan
)


class ReloadResponse(BaseModel):
    status: str
    message: str
    active_version: str


# --- Health Endpoints ---
@app.get("/health")
async def health():
    return {"status": "healthy"}


# --- Prediction Endpoint ---
@app.post("/predict", response_model=PredictionResponse)
async def predict_energy(payload: List[PredictionInput], request: Request, db: Session = Depends(db_utility.get_db)):

    if len(payload) > MAX_BATCH_SIZE:
        raise HTTPException(status_code=422, detail=f"Batch size {len(payload)} exceeds maximum of {MAX_BATCH_SIZE}")

    model = request.app.state.model
    energy_source_map = request.app.state.energy_source_map

    # safety check
    if model is None:
        raise HTTPException(status_code=500, detail="Prediction model not loaded")

    try:
        predictions = []
        db_input_records = []
        db_pred_records = []
        model_version = getattr(model, "version", "unknown")
        now = datetime.now(timezone.utc)

        for row in payload:
            energy_source_id = energy_source_map.get(row.energy_source)
            if energy_source_id is None:
                raise HTTPException(status_code=422, detail=f"Unknown energy_source: '{row.energy_source}'")

            features = {
                "date": row.date,
                "start_hour": row.start_hour,
                "end_hour": row.end_hour,
                "energy_source": row.energy_source,
            }

            prediction = model.predict(features)

            # prepare response
            predictions.append(
                Prediction(
                    production=prediction,
                    model_version=model_version,
                    received_input=row
                )
            )

            # prepare input_data row (full feature vector for drift detection)
            temporal = derive_temporal_features(row.date)
            db_input_records.append({
                "energy_source_id": energy_source_id,
                "input_date": row.date,
                "start_hour": row.start_hour,
                "end_hour": row.end_hour,
                "day_of_year": temporal["day_of_year"],
                "day_name": temporal["day_name"],
                "month_name": temporal["month_name"],
                "season": temporal["season"],
            })

            # prepare prediction row
            db_pred_records.append({
                "energy_source_id": energy_source_id,
                "input_date": row.date,
                "input_time_start": row.start_hour,
                "input_time_end": row.end_hour,
                "predict_result": prediction,
                "prediction_source": row.prediction_source,
                "predict_date": now,
                "ml_model": model_version,
            })

        # write both tables in one transaction
        db_utility.save_input_and_predictions_batch(db, db_input_records, db_pred_records)

        return {"status": "success", "predictions": predictions}

    except HTTPException:
        raise

    except Exception as e:
        print(f"Prediction error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Prediction failed due to internal error"
        )


# --- Model Reload Endpoint ---
@app.post("/reload-model", response_model=ReloadResponse)
async def reload_model(request: Request):
    """
    Triggered by Airflow after a successful model promotion.
    Forces the API to fetch the latest @champion from MLflow.
    """
    try:
        # Reload the model using the updated predict.py logic
        request.app.state.model = predict.load_model()

        model_version = getattr(request.app.state.model, "version", "unknown")

        return ReloadResponse(
            status="success",
            message="Model successfully hot-swapped from MLflow Registry.",
            active_version=model_version
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reload model from MLflow: {str(e)}"
        )


@app.get("/past-predictions")
async def get_history(
    request: Request,
    ml_model: Optional[str] = None,
    energy_source: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    prediction_source: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(db_utility.get_db)
):
    energy_name_to_id = request.app.state.energy_source_map
    energy_id_to_name = {v: k for k, v in energy_name_to_id.items()}
    energy_source_id = None
    if energy_source is not None:
        energy_source_id = energy_name_to_id.get(energy_source)
        if energy_source_id is None:
            raise HTTPException(status_code=404, detail=f"Energy source '{energy_source}' not found")
    records = db_utility.query_predictions(
        db=db,
        ml_model=ml_model,
        energy_source_id=energy_source_id,
        start_date=start_date,
        end_date=end_date,
        prediction_source=prediction_source,
        limit=limit
    )
    predictions = [
        {
            "production": r.predict_result,
            "model_version": r.ml_model,
            "received_input": {
                "date": r.predict_date,
                "input_date": r.input_date,
                "start_hour": r.input_time_start,
                "end_hour": r.input_time_end,
                "energy_source": energy_id_to_name.get(r.energy_source_id),
                "prediction_source": r.prediction_source,
            }
        }
        for r in records
    ]
    return {"status": "success", "predictions": predictions}

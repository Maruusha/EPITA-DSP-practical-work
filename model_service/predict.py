import random
from datetime import datetime
import db_utility

USE_TEMP_MODEL = True


def _temp_model(feature_row) -> tuple:
    """Returns a random float and the model version."""
    prediction = round(random.uniform(1000.0, 5000.0), 1)
    model_version = "0.1"
    return prediction, model_version


def real_model(feature_row):
    # TODO: Implement real ML logic here
    pass


def predict(feature_row):
    """Routes to the real model or temp model."""
    if USE_TEMP_MODEL:
        return _temp_model(feature_row)
    return real_model(feature_row)


def fetch_source_ids(features: list) -> dict:
    """Fetches the source name → ID map from the DB for the given features."""
    return db_utility.get_source_ids_by_names([f.energy_source for f in features])


def build_db_row(row, s_id: int, prediction: float, model_version: str) -> dict:
    """Constructs a single DB row dict from a feature row and its prediction."""
    return {
        "input_source_id": s_id,
        "input_date": datetime.strptime(row.date, "%Y-%m-%d"),
        "input_time_start": row.start_hour,
        "input_time_end": row.end_hour,
        "predict_result": prediction,
        "predict_date": datetime.utcnow(),
        "ml_model": model_version
    }


def write_predictions(features: list, predictions: list) -> None:
    """Fetches source IDs, builds DB rows, and saves to DB."""
    source_id_map = _fetch_source_ids(features)
    db_rows = []

    for row, (prediction, model_version) in zip(features, predictions):
        s_id = source_id_map.get(row.energy_source)
        if s_id is None:
            print(f"Warning: Source '{row.energy_source}' not found in database. Skipping row.")
            continue
        db_rows.append(_build_db_row(row, s_id, prediction, model_version))

    if db_rows:
        db_utility.save_predictions_batch(db_rows)


def process_and_log(features: list) -> list:
    """
    Takes a list of Pydantic objects from main.py,
    gets predictions, and logs to DB.
    """
    predictions = [predict(row) for row in features]
    #write_predictions(features, predictions)

    return [
        {"prediction": prediction, "model_version": model_version, "received_input": row.dict()}
        for row, (prediction, model_version) in zip(features, predictions)
    ]
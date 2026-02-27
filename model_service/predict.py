import random
from datetime import datetime
import db_utility

USE_TEMP_MODEL = True

def _temp_model(feature_row) -> tuple:
    """Returns a random float and the model version."""
    prediction = round(random.uniform(1000.0, 5000.0), 1)
    model_version = "0.1"
    return prediction, model_version

def _real_model(feature_row):
    # TODO: Implement real ML logic here
    pass

def predict(feature_row):
    """Routes to the real model or temp model."""
    if USE_TEMP_MODEL:
        return _temp_model(feature_row)
    return _real_model(feature_row)

def process_and_log(features: list):
    """
    Takes a list of Pydantic objects from main.py,
    gets predictions, and logs to DB.
    """
    # 1. Get the map of names to IDs from the DB
    source_id_map = db_utility.get_source_ids_by_names([f.energy_source for f in features])

    results_to_return = []
    db_rows = []

    for row in features:
        # Match the Streamlit key 'energy_source'
        s_name = row.energy_source 
        s_id = source_id_map.get(s_name)

        if s_id is None:
            print(f"Warning: Source '{s_name}' not found in database. Skipping row.")
            continue

        # 2. Get prediction and version
        prediction, model_version = predict(row)
        
        # Format the return for main.py (and eventually Streamlit)
        results_to_return.append({
            "prediction": prediction,
            "model_version": model_version,
            "received_input": row.dict() 
        })

        # 3. Prepare row for SQLAlchemy
        db_rows.append({
            "input_source_id": s_id,
            "input_date": datetime.strptime(row.date, "%Y-%m-%d"),
            "input_time_start": row.start_hour,
            "input_time_end": row.end_hour,
            "predict_result": prediction,
            "predict_date": datetime.utcnow(),
            "ml_model": model_version
        })

    if db_rows:
        db_utility.save_predictions_batch(db_rows)
        
    return results_to_return
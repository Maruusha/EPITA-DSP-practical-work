import os
import requests

REP_PREDICT_API_URL = os.getenv("REP_PREDICT_API_URL", "http://localhost:8080")

def make_prediction(payload: dict):
    try:
        response = requests.post(
            f"{REP_PREDICT_API_URL}/predict",
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}
import os
import requests

# Renewable Energy Prediction API URL
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
    
def get_past_predictions(params):

    try:
        response = requests.get(
            f"{REP_PREDICT_API_URL}/past-predictions",
            params=params,
            timeout=10
        )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def check_health():
    """Pings the API health endpoint and returns the status."""
    try:
        response = requests.get(f"{REP_PREDICT_API_URL}/health", timeout=2)
        if response.status_code == 200:
            return {"status": "online", "code": 200}
        else:
            return {"status": "error", "code": response.status_code}
    except requests.exceptions.RequestException:
        return {"status": "offline", "code": None}
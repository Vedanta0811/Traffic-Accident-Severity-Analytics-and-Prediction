"""
Automated Unit and Integration Tests for Pipeline & FastAPI Microservice.
"""

import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import numpy as np
import pandas as pd
from fastapi.testclient import TestClient
from src.etl.db import get_engine, init_db
from src.etl.validation import validate_and_quarantine
from src.api.main import app, get_model


@pytest.fixture
def client():
    return TestClient(app)


def test_database_init():
    """Verify database engine connectivity and schema initialization."""
    engine = get_engine()
    init_db(engine)
    assert engine is not None


def test_data_validation_and_quarantine():
    """Verify data quality checks separate valid records from corrupted anomalies."""
    res = validate_and_quarantine()
    assert "total" in res
    assert "valid" in res
    assert "rejected" in res
    assert res["total"] >= res["valid"]


def test_model_pipeline_predict():
    """Verify production model pipeline loads and returns valid predictions."""
    model = get_model()
    assert model is not None
    
    sample_df = pd.DataFrame([{
        "hour": 17,
        "speed_limit": 30,
        "temperature_c": 12.0,
        "precipitation_mm": 1.5,
        "visibility_m": 8000.0,
        "wind_speed_kmh": 20.0,
        "number_of_vehicles": 2,
        "number_of_casualties": 1,
        "hour_sin": np.sin(2 * np.pi * 17 / 24.0),
        "hour_cos": np.cos(2 * np.pi * 17 / 24.0),
        "day_name": "Friday",
        "time_of_day": "Evening Rush",
        "urban_or_rural": "Urban",
        "road_type": "Single carriageway",
        "light_conditions": "Daylight",
        "road_surface_conditions": "Wet or damp",
        "weather_condition": "Raining no high winds"
    }])
    
    pred = model.predict(sample_df)
    proba = model.predict_proba(sample_df)
    
    assert len(pred) == 1
    assert pred[0] in [0, 1, 2]
    assert np.isclose(np.sum(proba[0]), 1.0, atol=1e-3)


def test_api_health_endpoint(client):
    """Verify FastAPI /health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert data["model_loaded"] is True


def test_api_prediction_endpoint(client):
    """Verify FastAPI /predict endpoint with Pydantic payload."""
    payload = {
        "hour": 18,
        "day_name": "Friday",
        "time_of_day": "Evening Rush",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "urban_or_rural": "Urban",
        "road_type": "Single carriageway",
        "speed_limit": 30,
        "light_conditions": "Daylight",
        "road_surface_conditions": "Wet or damp",
        "weather_condition": "Raining no high winds",
        "temperature_c": 10.0,
        "precipitation_mm": 2.0,
        "visibility_m": 5000.0,
        "wind_speed_kmh": 22.0,
        "number_of_vehicles": 2,
        "number_of_casualties": 1
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "predicted_severity_code" in data
    assert "predicted_severity_label" in data
    assert data["predicted_severity_label"] in ["Fatal", "Serious", "Slight"]
    assert 0.0 <= data["severity_risk_score"] <= 1.0

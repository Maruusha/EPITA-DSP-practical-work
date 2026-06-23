from datetime import date

import pytest
from pydantic import ValidationError

from DataValClass import DataValClass
from schemas import PredictionInput


def test_valid_prediction_input():
    obj = PredictionInput(
        date=date(2024, 6, 1),
        start_hour=8,
        end_hour=12,
        energy_source="Solar",
        prediction_source="webapp",
    )
    assert obj.energy_source == "Solar"
    assert obj.start_hour == 8
    assert obj.end_hour == 12


def test_start_hour_above_23_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=24,
            end_hour=25,
            energy_source="Solar",
            prediction_source="webapp",
        )


def test_negative_start_hour_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=-1,
            end_hour=8,
            energy_source="Wind",
            prediction_source="webapp",
        )


# ---------------------------------------------------------------------------
# PredictionInput — hour order validation
# ---------------------------------------------------------------------------

def test_start_hour_equal_to_end_hour_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=10,
            end_hour=10,
            energy_source="Solar",
            prediction_source="webapp",
        )


def test_start_hour_greater_than_end_hour_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=15,
            end_hour=10,
            energy_source="Wind",
            prediction_source="webapp",
        )


# ---------------------------------------------------------------------------
# PredictionInput — string field validation
# ---------------------------------------------------------------------------

def test_blank_energy_source_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=8,
            end_hour=12,
            energy_source="   ",
            prediction_source="webapp",
        )


def test_blank_prediction_source_is_invalid():
    with pytest.raises(ValidationError):
        PredictionInput(
            date=date(2024, 6, 1),
            start_hour=8,
            end_hour=12,
            energy_source="Solar",
            prediction_source="  ",
        )


# ---------------------------------------------------------------------------
# DataValClass — defaults and construction
# ---------------------------------------------------------------------------

def test_data_val_class_defaults():
    obj = DataValClass()
    assert obj.error_criticality == "None"
    assert obj.is_schema_valid is True
    assert obj.error_count == 0
    assert obj.error_rate == 0.0
    assert obj.total_rows == 0
    assert obj.is_processed is False


def test_data_val_class_with_records():
    obj = DataValClass(
        records=[{"Date": "01/01/2024", "Production": "1000"}],
        source_filename="test.csv",
        total_rows=1,
    )
    assert obj.total_rows == 1
    assert obj.source_filename == "test.csv"
    assert len(obj.records) == 1


def test_data_val_class_high_criticality_state():
    obj = DataValClass(
        records=[{"col": "a"}, {"col": "b"}],
        bad_records=[{"col": "b"}],
        total_rows=2,
        error_count=1,
        error_rate=0.5,
        error_criticality="Medium",
    )
    assert len(obj.bad_records) == 1
    assert obj.error_criticality == "Medium"


def test_data_val_class_schema_invalid_state():
    obj = DataValClass(
        total_rows=5,
        error_count=5,
        error_rate=1.0,
        is_schema_valid=False,
        error_criticality="High",
        schema_missing_column=["Production"],
        schema_missing_column_count=1,
    )
    assert obj.is_schema_valid is False
    assert obj.error_criticality == "High"
    assert "Production" in obj.schema_missing_column

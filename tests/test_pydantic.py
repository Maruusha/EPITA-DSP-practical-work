import pytest
from datetime import date
from pydantic import ValidationError
from DataValClass import DataValClass
from main import PredictionInput


class TestDataValClass:
    def test_default_values(self):
        obj = DataValClass()
        assert obj.error_rate == 0
        assert obj.is_schema_valid is True
        assert obj.error_criticality == "None"
        assert obj.total_rows == 0
        assert obj.records == []

    def test_set_fields(self):
        obj = DataValClass(source_filename="test.csv", total_rows=100, error_count=10, error_rate=0.1)
        assert obj.source_filename == "test.csv"
        assert obj.total_rows == 100
        assert obj.error_count == 10

    def test_model_dump_roundtrip(self):
        obj = DataValClass(source_filename="file.csv", total_rows=50)
        restored = DataValClass(**obj.model_dump())
        assert restored.source_filename == "file.csv"
        assert restored.total_rows == 50


class TestPredictionInput:
    def _valid(self):
        return {
            "date": date(2024, 6, 1),
            "start_hour": 8,
            "end_hour": 12,
            "energy_source": "Solar",
            "prediction_source": "manual",
        }

    def test_valid_input_accepted(self):
        obj = PredictionInput(**self._valid())
        assert obj.start_hour == 8
        assert obj.end_hour == 12

    def test_empty_energy_source_raises(self):
        data = {**self._valid(), "energy_source": "   "}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

    def test_empty_prediction_source_raises(self):
        data = {**self._valid(), "prediction_source": ""}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

    def test_hour_above_23_raises(self):
        data = {**self._valid(), "end_hour": 25}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

    def test_hour_below_0_raises(self):
        data = {**self._valid(), "start_hour": -1}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

    def test_start_equal_to_end_raises(self):
        data = {**self._valid(), "start_hour": 10, "end_hour": 10}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

    def test_start_greater_than_end_raises(self):
        data = {**self._valid(), "start_hour": 15, "end_hour": 8}
        with pytest.raises(ValidationError):
            PredictionInput(**data)

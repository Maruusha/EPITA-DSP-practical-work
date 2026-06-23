import pytest
from validation_utils import calculate_criticality


def test_high_when_schema_invalid():
    assert calculate_criticality(0.0, is_schema_valid=False) == "High"


def test_high_when_schema_invalid_regardless_of_rate():
    assert calculate_criticality(0.05, is_schema_valid=False) == "High"


def test_high_when_error_rate_above_50():
    assert calculate_criticality(0.51, is_schema_valid=True) == "High"


def test_medium_at_lower_boundary():
    assert calculate_criticality(0.10, is_schema_valid=True) == "Medium"


def test_medium_at_upper_boundary():
    assert calculate_criticality(0.50, is_schema_valid=True) == "Medium"


def test_medium_in_range():
    assert calculate_criticality(0.30, is_schema_valid=True) == "Medium"


def test_low_when_error_rate_below_10():
    assert calculate_criticality(0.05, is_schema_valid=True) == "Low"


def test_none_when_no_errors():
    assert calculate_criticality(0.0, is_schema_valid=True) == "None"

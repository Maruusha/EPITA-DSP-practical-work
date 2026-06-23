from validation_utils import compute_criticality


def test_high_criticality_above_50_percent():
    assert compute_criticality(0.51, is_schema_valid=True) == "High"


def test_high_criticality_all_invalid():
    assert compute_criticality(1.0, is_schema_valid=True) == "High"


def test_high_criticality_invalid_schema_overrides_zero_error_rate():
    assert compute_criticality(0.0, is_schema_valid=False) == "High"


def test_high_criticality_invalid_schema_with_low_error_rate():
    assert compute_criticality(0.05, is_schema_valid=False) == "High"


def test_medium_criticality_middle_range():
    assert compute_criticality(0.25, is_schema_valid=True) == "Medium"


def test_medium_criticality_at_lower_boundary():
    assert compute_criticality(0.10, is_schema_valid=True) == "Medium"


def test_medium_criticality_at_upper_boundary():
    assert compute_criticality(0.50, is_schema_valid=True) == "Medium"


def test_low_criticality():
    assert compute_criticality(0.05, is_schema_valid=True) == "Low"


def test_low_criticality_just_above_zero():
    assert compute_criticality(0.01, is_schema_valid=True) == "Low"


def test_none_criticality_no_errors():
    assert compute_criticality(0.0, is_schema_valid=True) == "None"

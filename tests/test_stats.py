import pytest
from validation_utils import compute_error_stats


def test_error_count_matches_bad_indices():
    bad_indices = {0, 2, 4}
    error_count, error_rate = compute_error_stats(bad_indices, total_rows=10, is_schema_valid=True)
    assert error_count == 3
    assert error_rate == pytest.approx(0.3)


def test_error_rate_above_50():
    bad_indices = set(range(6))
    error_count, error_rate = compute_error_stats(bad_indices, total_rows=10, is_schema_valid=True)
    assert error_count == 6
    assert error_rate == pytest.approx(0.6)


def test_schema_invalid_forces_all_errors():
    bad_indices = {0}
    error_count, error_rate = compute_error_stats(bad_indices, total_rows=10, is_schema_valid=False)
    assert error_count == 10
    assert error_rate == 1.0


def test_zero_errors():
    error_count, error_rate = compute_error_stats(set(), total_rows=10, is_schema_valid=True)
    assert error_count == 0
    assert error_rate == 0.0


def test_zero_total_rows_does_not_divide_by_zero():
    error_count, error_rate = compute_error_stats(set(), total_rows=0, is_schema_valid=True)
    assert error_rate == 0.0

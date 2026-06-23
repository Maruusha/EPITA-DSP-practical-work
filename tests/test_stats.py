from validation_utils import compute_error_stats


def test_stats_no_errors():
    count, rate = compute_error_stats(bad_indices=set(), total_rows=100, is_schema_valid=True)
    assert count == 0
    assert rate == 0.0


def test_stats_some_errors():
    count, rate = compute_error_stats(bad_indices={0, 1, 2}, total_rows=10, is_schema_valid=True)
    assert count == 3
    assert rate == 0.3


def test_stats_all_errors():
    count, rate = compute_error_stats(bad_indices={0, 1, 2, 3, 4}, total_rows=5, is_schema_valid=True)
    assert count == 5
    assert rate == 1.0


def test_stats_over_50_percent_triggers_high_threshold():
    count, rate = compute_error_stats(bad_indices={0, 1, 2, 3, 4, 5}, total_rows=10, is_schema_valid=True)
    assert count == 6
    assert rate > 0.50


def test_stats_invalid_schema_forces_all_rows_as_errors():
    count, rate = compute_error_stats(bad_indices=set(), total_rows=20, is_schema_valid=False)
    assert count == 20
    assert rate == 1.0


def test_stats_zero_rows_no_division_error():
    count, rate = compute_error_stats(bad_indices=set(), total_rows=0, is_schema_valid=True)
    assert count == 0
    assert rate == 0.0

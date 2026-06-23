import pytest
import pandas as pd
from validation_utils import split_records


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": ["a", "b", "c", "d", "e"]
    })


def test_good_and_bad_rows_are_separated(sample_df):
    bad_indices = {1, 3}
    good, bad = split_records(sample_df, bad_indices, is_schema_valid=True)
    assert len(good) == 3
    assert len(bad) == 2


def test_bad_row_content_is_correct(sample_df):
    bad_indices = {0}
    good, bad = split_records(sample_df, bad_indices, is_schema_valid=True)
    assert bad[0]["id"] == 1


def test_all_records_are_bad_when_schema_invalid(sample_df):
    bad_indices = {1}
    good, bad = split_records(sample_df, bad_indices, is_schema_valid=False)
    assert len(good) == 0
    assert len(bad) == 5


def test_all_good_when_no_errors(sample_df):
    good, bad = split_records(sample_df, set(), is_schema_valid=True)
    assert len(good) == 5
    assert len(bad) == 0


def test_good_plus_bad_equals_total(sample_df):
    bad_indices = {0, 2}
    good, bad = split_records(sample_df, bad_indices, is_schema_valid=True)
    assert len(good) + len(bad) == len(sample_df)

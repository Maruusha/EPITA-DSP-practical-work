import pandas as pd

from validation_utils import split_records


def _make_df():
    return pd.DataFrame([
        {"col": "a", "val": 1},
        {"col": "b", "val": 2},
        {"col": "c", "val": 3},
        {"col": "d", "val": 4},
    ])


def test_split_no_bad_rows():
    df = _make_df()
    good, bad = split_records(df, bad_indices=set(), is_schema_valid=True)
    assert len(good) == 4
    assert len(bad) == 0


def test_split_some_bad_rows():
    df = _make_df()
    good, bad = split_records(df, bad_indices={1, 3}, is_schema_valid=True)
    assert len(good) == 2
    assert len(bad) == 2


def test_split_all_bad_rows():
    df = _make_df()
    good, bad = split_records(df, bad_indices={0, 1, 2, 3}, is_schema_valid=True)
    assert len(good) == 0
    assert len(bad) == 4


def test_split_invalid_schema_sends_everything_to_bad():
    df = _make_df()
    good, bad = split_records(df, bad_indices=set(), is_schema_valid=False)
    assert len(good) == 0
    assert len(bad) == 4


def test_split_invalid_schema_ignores_bad_indices():
    df = _make_df()
    good, bad = split_records(df, bad_indices={0}, is_schema_valid=False)
    assert len(good) == 0
    assert len(bad) == 4


def test_split_correct_rows_separated():
    df = _make_df()
    good, bad = split_records(df, bad_indices={2}, is_schema_valid=True)
    good_vals = [r["col"] for r in good]
    bad_vals = [r["col"] for r in bad]
    assert "c" not in good_vals
    assert "c" in bad_vals
    assert set(good_vals) == {"a", "b", "d"}

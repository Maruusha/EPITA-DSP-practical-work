import pandas as pd


def compute_criticality(error_rate: float, is_schema_valid: bool) -> str:
    """Return criticality level based on error rate and schema validity."""
    if not is_schema_valid or error_rate > 0.50:
        return "High"
    elif 0.10 <= error_rate <= 0.50:
        return "Medium"
    elif 0 < error_rate < 0.10:
        return "Low"
    return "None"


def compute_error_stats(bad_indices: set, total_rows: int, is_schema_valid: bool) -> tuple:
    """Return (error_count, error_rate). Schema invalidity forces all rows to be errors."""
    if not is_schema_valid:
        return total_rows, 1.0
    error_count = len(bad_indices)
    error_rate = error_count / total_rows if total_rows > 0 else 0.0
    return error_count, error_rate


def split_records(df: pd.DataFrame, bad_indices: set, is_schema_valid: bool) -> tuple:
    """Return (good_records, bad_records) as lists of dicts.

    When schema is invalid all records go to bad_records.
    """
    bad_df = df.iloc[list(bad_indices)]
    good_df = df.drop(index=list(bad_indices))
    if is_schema_valid:
        return good_df.to_dict(orient="records"), bad_df.to_dict(orient="records")
    return [], df.to_dict(orient="records")  # If data has schema error, all data is bad

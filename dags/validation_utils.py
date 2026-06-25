import pandas as pd

# Maps GX expectation type → semantic error category
EXPECTATION_CATEGORY_MAP = {
    "expect_column_values_to_not_be_null": "completeness",
    "expect_column_values_to_be_in_set": "validity",
    "expect_column_values_to_be_between": "range_violation",
    "expect_table_columns_to_match_set": "schema",
    "expect_column_to_exist": "schema",
}


def detect_type_errors(df: pd.DataFrame, numeric_cols: list, date_col: str, null_sentinel: str = 'nan_value') -> dict:
    """
    Scans the raw (pre-coercion) DataFrame for values that are non-null but cannot be
    parsed as their expected type.  Returns {col: set_of_row_indices}.

    Must be called before any pd.to_numeric / pd.to_datetime coercion so that 'xyz'
    type errors are still visible as strings instead of NaN.
    """
    type_error_map = {}
    for col in numeric_cols:
        if col not in df.columns:
            continue
        is_non_null = df[col].astype(str) != null_sentinel
        is_non_parseable = pd.to_numeric(df[col], errors='coerce').isna()
        bad_mask = is_non_null & is_non_parseable
        indices = set(df.index[bad_mask].tolist())
        if indices:
            type_error_map[col] = indices

    if date_col in df.columns:
        is_non_null = df[date_col].astype(str) != null_sentinel
        is_non_parseable = pd.to_datetime(df[date_col], format='%m/%d/%Y', errors='coerce').isna()
        bad_mask = is_non_null & is_non_parseable
        indices = set(df.index[bad_mask].tolist())
        if indices:
            type_error_map[date_col] = indices

    return type_error_map


def get_failing_indices(df: pd.DataFrame, r) -> list:
    """
    Returns the integer row indices for a failed GX expectation result.

    First tries GX's own unexpected_index_list (populated only when result_format
    is COMPLETE AND the GX version supports it).  Falls back to an equivalent pandas
    check so that row numbers are always available regardless of GX version quirks.

    The DataFrame passed in must already be coerced (to_numeric / to_datetime applied)
    so that null/range comparisons match what GX actually validated against.
    """
    # --- GX-provided list (may be empty in GX 1.x without explicit index config) ---
    raw = (r.result or {}).get('unexpected_index_list') or []
    if raw:
        # GX 1.x sometimes returns [{'index_col': value}, ...] instead of plain ints
        if isinstance(raw[0], dict):
            return [int(list(v.values())[0]) for v in raw]
        return [int(i) for i in raw]

    # --- Pandas fallback ---
    exp_type = r.expectation_config.type
    kwargs = getattr(r.expectation_config, 'kwargs', {}) or {}
    col = kwargs.get('column')

    if not col or col not in df.columns:
        return []

    try:
        if exp_type == "expect_column_values_to_not_be_null":
            mask = df[col].isna()

        elif exp_type == "expect_column_values_to_be_in_set":
            value_set = set(kwargs.get('value_set', []))
            mask = df[col].notna() & ~df[col].isin(value_set)

        elif exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get('min_value')
            max_val = kwargs.get('max_value')
            not_null = df[col].notna()
            below = (df[col] < min_val) if min_val is not None else pd.Series(False, index=df.index)
            above = (df[col] > max_val) if max_val is not None else pd.Series(False, index=df.index)
            mask = not_null & (below | above)

        else:
            return []

        return [int(i) for i in df.index[mask].tolist()]
    except Exception:
        return []


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

from pydantic import BaseModel
from typing import List, Dict

class DataValClass(BaseModel):
    # actual data
    records: List[Dict] = []
    good_records: List[Dict] = []
    bad_records: List[Dict] = []
    
    # File info
    source_filename: str = "no_file"
    is_processed: bool = False
    error_criticality: str = "None"

    # Validation info
    report_url: str = ""
    is_schema_valid: bool = True
    schema_missing_column: List[str] = []
    schema_missing_column_count: int = 0
    total_rows: int = 0
    error_count: int = 0
    error_rate: float = 0

    # Per-category error counts (for Grafana dashboard)
    missing_values_count: int = 0       # completeness
    type_error_count: int = 0           # type
    outlier_error_count: int = 0        # validity (out of range)
    consistency_error_count: int = 0    # consistency (invalid category values)
    schema_error_count: int = 0         # schema (missing columns)

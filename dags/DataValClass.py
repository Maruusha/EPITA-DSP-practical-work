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
    is_schema_valid: bool = True
    schema_missing_column: List[str] = []
    total_rows: int = 0
    error_count: int = 0
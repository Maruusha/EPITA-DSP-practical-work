from pydantic import BaseModel
from typing import List, Dict

class DataValClass(BaseModel):
    # actual data
    records: List[Dict]
    good_records: List[Dict]
    bad_records: List[Dict]
    
    # File info
    source_filename: str
    is_processed: bool = False
    error_criticality: str
    
    # Validation info
    is_schema_valid: bool = True
    total_rows: int = 0
    error_count: int = 0
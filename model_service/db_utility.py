import os
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Pulls from the environment variable constructed in docker-compose.yml
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Table Definitions ---

class EnergySource(Base):
    __tablename__ = "energy_sources"
    id = Column(Integer, primary_key=True)
    source_type = Column(String, unique = True, nullable = False)  # Solar, Wind, Mix, etc.

class PredictionRecord(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    energy_source_id = Column(Integer, ForeignKey("energy_sources.id"))
    prediction_source = Column(String)
    input_date = Column(DateTime)
    input_time_start = Column(Integer)
    input_time_end = Column(Integer)
    predict_result = Column(Float)
    predict_date = Column(DateTime, default=datetime.utcnow)
    ml_model = Column(String)

class DataQualityStat(Base):
    __tablename__ = "data_quality_stats"
    
    # The Primary Key
    id = Column(Integer, primary_key=True, index=True)
    
    # File name column
    file_name = Column(String, index=True)
    
    # Time-Series Tracking column
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)
    
    # additional info about the bad dataset
    total_rows_processed = Column(Integer)
    total_clean_rows = Column(Integer)
    total_corrupt_rows = Column(Integer)
    
    # The detailed error columns
    missing_values_count = Column(Integer)
    type_error_count = Column(Integer)
    outlier_error_count = Column(Integer)

# --- Database Operations ---

def test_db_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return True
    except Exception:
        return False

def get_source_ids_by_names(names: list):
    db = SessionLocal()
    try:
        records = db.query(EnergySource).filter(EnergySource.source_type.in_(names)).all()
        return {r.source_type: r.id for r in records}
    finally:
        db.close()

def save_predictions_batch(records_data: list):
    db = SessionLocal()
    try:
        db_records = [PredictionRecord(**data) for data in records_data]
        db.add_all(db_records)
        db.commit()
    finally:
        db.close()

def query_predictions(ml_model=None, energy_source_id=None, start_date=None, end_date=None, limit=100):
    db = SessionLocal()
    try:
        query = db.query(PredictionRecord)
        if ml_model:
            query = query.filter(PredictionRecord.ml_model == ml_model)
        if energy_source_id:
            query = query.filter(PredictionRecord.energy_source_id == energy_source_id)
        if start_date:
            query = query.filter(PredictionRecord.predict_date >= start_date)
        if end_date:
            query = query.filter(PredictionRecord.predict_date <= end_date)
        
        return query.order_by(PredictionRecord.predict_date.desc()).limit(limit).all()
    finally:
        db.close()
import db_utility
import os
from db_utility import EnergySource, SessionLocal
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

# Creating airflow databases if not exist
AIRFLOW_DATABASE_URL = os.getenv("AIRFLOW_DATABASE_URL")
db_utility.create_database_if_not_exists(AIRFLOW_DATABASE_URL)

print("Initializing database tables...")
db_utility.Base.metadata.create_all(bind=db_utility.engine, checkfirst=True)

# Additive column migrations — create_all won't alter existing tables
with db_utility.engine.connect() as _conn:
    _conn.execute(text(
        "ALTER TABLE data_quality_errors ADD COLUMN IF NOT EXISTS error_category VARCHAR"
    ))

    # training_statistics — new columns for covariate, target, and concept drift
    for col_ddl in [
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS row_count INTEGER",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS production_min FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS production_max FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS production_p25 FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS production_p50 FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS production_p75 FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS start_hour_mean FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS start_hour_std FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS end_hour_mean FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS end_hour_std FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS day_of_year_mean FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS day_of_year_std FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS wind_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS mixed_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS spring_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS summer_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS fall_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS winter_percentage FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS solar_production_mean FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS wind_production_mean FLOAT",
        "ALTER TABLE training_statistics ADD COLUMN IF NOT EXISTS mixed_production_mean FLOAT",
    ]:
        _conn.execute(text(col_ddl))

    _conn.commit()

print("Step completed")

print("Seeding initial dictionary data...")
# Open a secure connection to the database
session = SessionLocal()

try:
    # source types that will be inserted if it doesnt exist in the table already
    sources_to_insert = [
        {"source_type": "Wind"},
        {"source_type": "Solar"},
        {"source_type": "Mixed"}
    ]

    # SQL alchemy upsert method used to upload the source type after checking it's existence
    stmt = insert(EnergySource).values(sources_to_insert)

    stmt = stmt.on_conflict_do_nothing(index_elements=['source_type'])

    session.execute(stmt)
    session.commit()

    print("Energy sources uploaded/verified successfully.")

except Exception as e:
    # The Safety Valve: If anything crashes, undo all partial changes
    print(f" Error during seeding: {e}")
    session.rollback()
finally:
    # Always close the connection to prevent memory leaks
    session.close()

print("Startup checks complete.")

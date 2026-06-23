import db_utility
import os
from db_utility import EnergySource, SessionLocal
from sqlalchemy.dialects.postgresql import insert

# Creating airflow databases if not exist
AIRFLOW_DATABASE_URL = os.getenv("AIRFLOW_DATABASE_URL")
db_utility.create_database_if_not_exists(AIRFLOW_DATABASE_URL)

print("Initializing database tables...")
db_utility.Base.metadata.create_all(bind=db_utility.engine, checkfirst=True)
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

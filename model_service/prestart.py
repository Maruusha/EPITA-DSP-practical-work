import db_utility
from db_utility import DictSourceName, SessionLocal

print("Initializing database tables...")
db_utility.Base.metadata.create_all(bind=db_utility.engine, checkfirst=True)
print("Step completed")

print("Seeding initial dictionary data...")
# Open a secure connection to the database
session = SessionLocal()

try:
    required_sources = ["Wind", "Solar", "Mixed"]

    for source in required_sources:
        # Check if the source already exists
        exists = session.query(DictSourceName).filter_by(source_type=source).first()
        
        # If it does not exist, queue it up to be saved
        if not exists:
            new_source = DictSourceName(source_type=source)
            session.add(new_source)

    # 'Push' the queued changes permanently into PostgreSQL
    session.commit()
    print("✅ Energy sources seeded successfully.")

except Exception as e:
    # The Safety Valve: If anything crashes, undo all partial changes
    print(f" Error during seeding: {e}")
    session.rollback()
finally:
    # Always close the connection to prevent memory leaks
    session.close()

print("Startup checks complete.")
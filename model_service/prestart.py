import db_utility

print("Initializing database tables...")
db_utility.Base.metadata.create_all(bind=db_utility.engine, checkfirst=True)
print("Done.")
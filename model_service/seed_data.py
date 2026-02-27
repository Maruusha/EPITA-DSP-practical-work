import pandas as pd
from db_utility import engine, SessionLocal, DictSourceName, HistoricalData
import datetime

def seed_database(csv_path):
    # 1. Load the dataset
    df = pd.read_csv(csv_path) 
    session = SessionLocal()
    
    try:
        # 2. Seed Energy Sources (Wind, Solar, Mixed)
        unique_sources = df['Source'].unique()
        for name in unique_sources:
            if not session.query(DictSourceName).filter_by(source_type=name).first():
                session.add(DictSourceName(source_type=name))
        session.commit()
        
        # Create the ID mapping
        source_map = {s.source_type: s.id for s in session.query(DictSourceName).all()}
        
        # 3. Seed Historical Data Table
        # Added chunksize=500 to prevent the parameter limit error
        print("⏳ Seeding historical_data...")
        hist_df = df.rename(columns={
            'Date': 'date', 'Start_Hour': 'start_hour', 'End_Hour': 'end_hour',
            'Source': 'source', 'Day_of_Year': 'day_of_year', 'Day_Name': 'day_name',
            'Month_Name': 'month_name', 'Season': 'season', 'Production': 'production'
        })
        hist_df.to_sql(
            'historical_data', 
            con=engine, 
            if_exists='append', 
            index=False, 
            chunksize=500, 
            method='multi'
        )
        
        # NOTE: Predictions seeding has been completely removed.
        # The 'predictions' table will now remain empty until the Streamlit UI is used.
        
        print(f"✅ Seeding complete!")
        print(f"   - 'Mixed' is mapped to ID: {source_map.get('Mixed')}")
        print("   - Successfully seeded energy_sources and historical_data. 'predictions' is ready for live API data!")

    except Exception as e:
        print(f"❌ Error during seeding: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    seed_database("Energy Production Dataset.csv")
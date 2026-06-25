import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

base_dir = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(base_dir, 'Energy Production Dataset.csv')
# output_folder = os.path.join(base_dir, 'raw_data')
# os.makedirs(output_folder, exist_ok=True)

df = pd.read_csv(dataset_path)
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

print("Original Source distribution:")
print(df['Source'].value_counts(normalize=True).round(3))

# CONFIGURATION

# Source drift: Solar grows, Wind shrinks, Mixed increases slightly year over year
# Baseline (2020-2025 average): Wind ~82%, Solar ~18%, Mixed ~0%
DRIFT_SOURCE_WEIGHTS = {
    2026: {'Wind': 0.77, 'Solar': 0.22, 'Mixed': 0.01},
    2027: {'Wind': 0.73, 'Solar': 0.26, 'Mixed': 0.01},
    2028: {'Wind': 0.68, 'Solar': 0.30, 'Mixed': 0.02},
    2029: {'Wind': 0.64, 'Solar': 0.34, 'Mixed': 0.02},
    2030: {'Wind': 0.59, 'Solar': 0.38, 'Mixed': 0.03},
}

# Concept drift: global structural shift in production (Solar boom, Wind decline)
DRIFT_PRODUCTION = 0.05

DRIFT_CONCEPT_PRODUCTION = {
    'Solar': 1.40,
    'Wind':  0.80,
    'Mixed': 1.15,
}

# Season drift: production multiplier per (season, source)
DRIFT_SEASON_PRODUCTION = {
    'Summer': {'Solar': 1.25, 'Wind': 1.10, 'Mixed': 1.05},
    'Winter': {'Solar': 0.90, 'Wind': 0.90, 'Mixed': 1.05},
    'Fall':   {'Solar': 1.10, 'Wind': 1.10, 'Mixed': 1.05},
    'Spring': {'Solar': 1.08, 'Wind': 0.90, 'Mixed': 1.05},
}

ROWS_PER_YEAR = 2000

SEASON_MAP = {
    12: 'Winter', 1: 'Winter', 2: 'Winter',
    3: 'Spring', 4: 'Spring', 5: 'Spring',
    6: 'Summer', 7: 'Summer', 8: 'Summer',
    9: 'Fall', 10: 'Fall', 11: 'Fall',
}

WEIGHT_NOISE = 0.02   # ± random noise applied to source weights and season multipliers

# ----- CODE -----

pools = {
    source: df[df['Source'] == source].reset_index(drop=True)
    for source in ['Wind', 'Solar', 'Mixed']
}

all_rows = []

start_year = min(DRIFT_SOURCE_WEIGHTS.keys())

for year, weights in DRIFT_SOURCE_WEIGHTS.items():
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    day_range = (end - start).days

    year_index = year - start_year + 1
    year_production_multiplier = (1 + DRIFT_PRODUCTION) ** year_index

    noise = np.random.uniform(-WEIGHT_NOISE, WEIGHT_NOISE)
    year_weights = weights.copy()
    year_weights['Wind'] = weights['Wind'] + noise
    year_weights['Solar'] = weights['Solar'] - noise

    sources = np.random.choice(
        list(year_weights.keys()),
        size=ROWS_PER_YEAR,
        p=list(year_weights.values())
    )

    for source in sources:
        date = start + timedelta(days=int(np.random.randint(0, day_range + 1)))
        sample = pools[source].sample(1).iloc[0]

        season = SEASON_MAP[date.month]
        season_multiplier = DRIFT_SEASON_PRODUCTION[season][source] + np.random.uniform(-WEIGHT_NOISE, WEIGHT_NOISE)
        production = sample['Production'] * year_production_multiplier * season_multiplier * DRIFT_CONCEPT_PRODUCTION[source] 

        all_rows.append({
            'Date': date.strftime('%m/%d/%Y'),
            'Start_Hour': sample['Start_Hour'],
            'End_Hour': sample['End_Hour'],
            'Source': source,
            'Day_of_Year': date.timetuple().tm_yday,
            'Day_Name': date.strftime('%A'),
            'Month_Name': date.strftime('%B'),
            'Season': season,
            'Production': int(production),
        })

# Output results 

drift_df = pd.DataFrame(all_rows)
drift_df = drift_df.sort_values('Date').reset_index(drop=True)
drift_df['Year'] = pd.to_datetime(drift_df['Date'], format='%m/%d/%Y').dt.year

for year, year_df in drift_df.groupby('Year'):
    output_path = os.path.join(base_dir, f'drift_{year}.csv')
    year_df.drop(columns='Year').to_csv(output_path, index=False)
    print(f"Saved {len(year_df)} rows -> {output_path}")

print("\nSource distribution per year:")
print(drift_df.groupby('Year')['Source'].value_counts(normalize=True).round(3))

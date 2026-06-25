import pandas as pd
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(base_dir, 'Energy Production Dataset.csv')
output_folder = os.path.join(base_dir, 'raw_data')
os.makedirs(output_folder, exist_ok=True)

num_files = 50  # increase for longer demos (each file = 1 ingestion run)
ROWS_PER_FILE = 10  # required: exactly 10 rows per file

# Load and shuffle
df = pd.read_csv(dataset_path)
df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Take only what we need (num_files * 10 rows)
df_trimmed = df_shuffled.head(num_files * ROWS_PER_FILE)

# Split into chunks of exactly 10 rows
for i in range(num_files):
    chunk = df_trimmed.iloc[i * ROWS_PER_FILE:(i + 1) * ROWS_PER_FILE]
    file_name = f"split_data_{i+1}.csv"
    file_path = os.path.join(output_folder, file_name)
    chunk.to_csv(file_path, index=False)
    print(f"Saved: {file_path} ({len(chunk)} rows)")

print(f"\nSuccessfully created {num_files} files with {ROWS_PER_FILE} rows each.")

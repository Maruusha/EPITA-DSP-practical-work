import pandas as pd
import os
import random

# save incase needed
# dataset_path = 'bad_data'
# output_folder = 'bad_data'

base_dir = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(base_dir, 'raw_data')
 
output_folder = os.path.join(base_dir, 'raw_data')
os.makedirs(output_folder, exist_ok=True)
num_corrupt_files = 3

col_list = ['Date', 'Start_Hour', 'End_Hour', 'Source', 'Day_of_Year', 'Day_Name',	'Month_Name', 'Season', 'Production']

# List of all files in raw_data
files = [f for f in os.listdir(dataset_path) if f.endswith('.csv')]
files = random.sample(files, 3)
print("List of dropped cols: " + str(files))

# SCHEMA ERROR
for file_name in files:
    file_path = os.path.join(dataset_path, file_name)
    print(file_path)

    num_corrupt_files = num_corrupt_files - 1
    if num_corrupt_files < 0:
        break

    df = pd.read_csv(file_path)

    drop_col = random.choice(col_list)
    df = df.drop(columns=[drop_col])
    print(f"Applied Schema Error to {file_name}: Dropped {drop_col}")

    # Save to raw_data
    output_path = os.path.join(output_folder, f"corrupted_schema_{file_name}")
    df.to_csv(output_path, index=False)
    print(f"Processed {file_name}: Corrupted {drop_col} rows -> {output_path}")
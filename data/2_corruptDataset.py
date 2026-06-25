import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# save incase needed
# dataset_path = 'raw_data'
# output_folder = 'raw_data'

base_dir = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(base_dir, 'raw_data')
 
output_folder = os.path.join(base_dir, 'raw_data')
os.makedirs(output_folder, exist_ok=True)

ERROR_PERCENTAGE = 0.15  # min 0 max 1s

# Define the possible error types (excluding Schema which is File-level)
row_error_types = ['completeness', 'validity', 'consistency', 'type', 'impossibleValue', 'outlier']
col_list = ['Date', 'Start_Hour', 'End_Hour', 'Source', 'Day_of_Year', 'Day_Name',	'Month_Name', 'Season', 'Production']

for file_name in os.listdir(dataset_path):
    if not file_name.endswith('.csv'):
        continue
        
    file_path = os.path.join(dataset_path, file_name)
    df = pd.read_csv(file_path)

    # IDENTIFY ROWS FOR CORRUPTION
    # Select a random subset of rows based on the error_rate
    total_rows = len(df)
    num_corrupt = int(total_rows * ERROR_PERCENTAGE)
    corrupt_indices = random.sample(range(total_rows), num_corrupt)
    two_years_from_now = (datetime.now() + timedelta(days=730)).strftime('%m/%d/%Y')

    for idx in range(num_corrupt):
        # Decide how many errors to apply to this row (1 to 3)
        num_errors_to_apply = random.randint(1, 3) 
        chosen_errors = random.sample(row_error_types, num_errors_to_apply)

        for error in chosen_errors:
            target_col = random.choice(col_list)
            if error == 'completeness':
                df.at[idx, target_col] = np.nan
            
            elif error == 'validity':
                df[target_col] = df[target_col].astype(object)
                df.at[idx, target_col] = 'xyz'
            
            elif error == 'consistency':
                df[target_col] = df[target_col].astype(object)
                df.at[idx, target_col] = 'xyz'
            
            elif error == 'type':
                df[target_col] = df[target_col].astype(object)
                df.at[idx, target_col] = "xyz"
                
            elif error == 'impossibleValue':
                df['Production'] = df['Production'].astype(object)
                df.at[idx, 'Production'] = -100
                df.at[idx, 'Date'] = two_years_from_now
 
            elif error == 'outlier':
                df['Production'] = df['Production'].astype(object)
                df.at[idx, 'Production'] = 40000

    # SHUFFLE THE ROWS
    df = df.sample(frac=1).reset_index(drop=True)

    # SAVE 
    output_path = os.path.join(output_folder, f"corrupted_{file_name}")
    df.to_csv(output_path, index=False)
    print(f"Processed {file_name}: Corrupted {num_corrupt} rows -> {output_path}")
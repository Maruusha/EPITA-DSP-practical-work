import pandas as pd
import numpy as np
import os
import random

dataset_path = 'raw_data'
output_folder = 'bad_data'
ERROR_PERCENTAGE = 0.15  # min 0 max 1

# Define the possible error types (excluding Schema which is File-level)
row_error_types = ['completeness', 'validity', 'consistency', 'type']

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

    # To allow multiple errors per row, we iterate through the chosen indices
    for idx in corrupt_indices:
        # Decide how many errors to apply to THIS specific row (1 to 3)
        num_errors_to_apply = random.randint(0, 3)
        chosen_errors = random.sample(row_error_types, num_errors_to_apply)

        for error in chosen_errors:
            if error == 'completeness':
                # Null values in required column
                df.at[idx, 'Source'] = np.nan
            
            elif error == 'validity':
                # Value outside valid range (e.g., Hour > 23 or Production < 0)
                df.at[idx, 'Start_Hour'] = 99
                df.at[idx, 'Production'] = -500
            
            elif error == 'consistency':
                # Invalid categorical value
                df.at[idx, 'Season'] = 'Nuclear_Winter'
            
            elif error == 'type':
                # Wrong data type (putting a string in a numeric column)
                # We cast to object first to prevent pandas from blocking the assignment
                df['Production'] = df['Production'].astype(object)
                df.at[idx, 'Production'] = "ERROR_VAL"

    # 4. SAVE TO BAD_DATA
    output_path = os.path.join(output_folder, f"corrupted_{file_name}")
    df.to_csv(output_path, index=False)
    print(f"Processed {file_name}: Corrupted {num_corrupt} rows -> {output_path}")
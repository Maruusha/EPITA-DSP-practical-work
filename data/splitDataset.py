import pandas as pd
import numpy as np
import os

dataset_path = 'Energy Production Dataset.csv'     
output_folder = 'raw_data'       
num_files = 10               

# Load the dataset
df = pd.read_csv(dataset_path)

# Randomly shuffle the entire dataframe
# frac=1 means take 100% of the data, random_state ensures reproducibility
df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Split the dataframe into N parts
# array_split handles cases where the division isn't even
chunks = np.array_split(df_shuffled, num_files)

# 4. Save each chunk to the output folder
for i, chunk in enumerate(chunks):
    file_name = f"split_data_{i+1}.csv"
    file_path = os.path.join(output_folder, file_name)
    
    chunk.to_csv(file_path, index=False)
    print(f"Saved: {file_path} ({len(chunk)} rows)")

print(f"\nSuccessfully split {len(df)} rows into {num_files} files.")
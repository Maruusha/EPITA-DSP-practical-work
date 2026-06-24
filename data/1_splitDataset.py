import pandas as pd
import numpy as np
import os

# save incase needed
# dataset_path = 'Energy Production Dataset.csv'     
# output_folder = 'raw_data'       

base_dir = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(base_dir, 'Energy Production Dataset.csv')
 
output_folder = os.path.join(base_dir, 'raw_data')
os.makedirs(output_folder, exist_ok=True)

num_files = 15               

# Load the dataset
df = pd.read_csv(dataset_path)

# Randomly shuffle the entire dataframe
# frac=1 means take 100% of the data, random_state ensures reproducibility
df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Split the dataframe into N parts
# array_split handles cases where the division isn't even
chunks = np.array_split(df_shuffled, num_files)

# 4. Save each chunk to the output folder
original_columns = df_shuffled.columns
for i, chunk in enumerate(chunks):
    file_name = f"split_data_{i+1}.csv"
    file_path = os.path.join(output_folder, file_name)
    df_chunk = pd.DataFrame(chunk, columns=original_columns)
    df_chunk.to_csv(file_path, index=False)
    print(f"Saved: {file_path} ({len(chunk)} rows)")

print(f"\nSuccessfully split {len(df)} rows into {num_files} files.")
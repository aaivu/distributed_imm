import os
import pandas as pd

# Path to the folder containing CSV files
folder_path = '/Users/bojitha/Desktop/FYP/exKMC/ExKMC/spark/op_5000000'

# Output file path
output_file = 'combined_output_5000000.csv'

# List to store individual DataFrames
csv_list = []

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    print(f"Processing file: {filename}")
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)
        csv_list.append(df)

# Concatenate all DataFrames
combined_df = pd.concat(csv_list, ignore_index=True)

# Save the combined DataFrame to a new CSV file
combined_df.to_csv(output_file, index=False)

print(f"Combined CSV saved as '{output_file}'")

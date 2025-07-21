# c:\ALCAMPO\python-algorithms\algortimo-gd\convert_columns_to_lowercase.py
import pandas as pd
import os

# Directory containing the CSV files
csv_dir = r'C:\ALCAMPO\python-algorithms\algortimo-gd\data\csvs'

# Get all CSV files in the directory
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

for file in csv_files:
    # Full path to the file
    file_path = os.path.join(csv_dir, file)
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    # Convert column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Save the file back with lowercase column names
    df.to_csv(file_path, index=False)
    print(f"Processed {file}: Column names converted to lowercase")
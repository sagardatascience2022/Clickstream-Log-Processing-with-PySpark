"""
Load clickstream data from local files (downloaded from Kaggle).
Usage: python scripts/load_kaggle.py [optional: path_to_file]
"""
import sys
import os
import traceback
import pandas as pd

def find_data_file(filename=None):
    """Look for data files in common locations."""
    search_paths = [
        ".",  # Current directory
        "data",  # data/ subdirectory
        os.path.join(os.path.dirname(__file__), '..', 'data'),  # ../data from script
    ]
    
    if filename:
        # Look for specific file
        for base in search_paths:
            path = os.path.join(base, filename)
            if os.path.isfile(path):
                return path
    else:
        # Look for any CSV/JSON files
        for base in search_paths:
            if os.path.isdir(base):
                for f in os.listdir(base):
                    if f.endswith(('.csv', '.json')):
                        return os.path.join(base, f)
    
    return None

def load_data(file_path):
    """Load data from CSV or JSON file."""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.json'):
        return pd.read_json(file_path, lines=True)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

# Get file path from argument or search
file_path = sys.argv[1] if len(sys.argv) > 1 else None
if not file_path or not os.path.isfile(file_path):
    file_path = find_data_file(file_path)

if not file_path:
    print("Error: Could not find data file.")
    print("Please either:")
    print("1. Pass the path to your data file as argument")
    print("2. Place a .csv or .json file in the data/ directory")
    sys.exit(1)

print(f"Loading data from: {file_path}")

try:
    df = load_data(file_path)
    print("\nLoaded DataFrame with shape:", df.shape)
    print("\nColumns:", df.columns.tolist())
    print("\nFirst 5 rows:")
    print(df.head())
    
except Exception as e:
    print("Failed to load data:")
    traceback.print_exc()
    sys.exit(2)

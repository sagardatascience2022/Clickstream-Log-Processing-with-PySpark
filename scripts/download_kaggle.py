"""
Download dataset from Kaggle using the Kaggle API.
Requires kaggle credentials in ~/.kaggle/kaggle.json or environment variables.
"""
import os
import sys
from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset(dataset_name, path="data"):
    """Download a dataset from Kaggle."""
    print(f"Authenticating with Kaggle...")
    api = KaggleApi()
    api.authenticate()
    
    print(f"Downloading dataset: {dataset_name}")
    try:
        api.dataset_download_files(
            dataset_name,
            path=path,
            unzip=True,
            quiet=False
        )
        print(f"Successfully downloaded to {path}/")
        
        # List downloaded files
        print("\nDownloaded files:")
        for f in os.listdir(path):
            print(f"- {f}")
            
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # The dataset we want to download
    DATASET = "waqi786/e-commerce-clickstream-and-transaction-dataset"
    
    # Create data directory if it doesn't exist
    if not os.path.exists("data"):
        os.makedirs("data")
        
    download_dataset(DATASET)
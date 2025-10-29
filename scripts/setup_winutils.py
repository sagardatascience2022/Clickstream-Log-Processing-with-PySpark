"""
Download winutils.exe for PySpark on Windows.
"""
import os
import sys
import urllib.request
import zipfile
import shutil

def download_winutils():
    """Download winutils.exe for Hadoop on Windows."""
    hadoop_dir = os.path.abspath("hadoop")
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    
    # URL for winutils.exe (Hadoop 3.3.5)
    winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    
    print(f"Downloading winutils.exe to {winutils_path}")
    urllib.request.urlretrieve(winutils_url, winutils_path)
    
    if os.path.exists(winutils_path):
        print("Successfully downloaded winutils.exe")
        print("\nAdd these environment variables:")
        print(f"HADOOP_HOME={hadoop_dir}")
        print(f"Add to PATH: {bin_dir}")
    else:
        print("Failed to download winutils.exe")
        sys.exit(1)

if __name__ == "__main__":
    download_winutils()
"""
Helper script to set up Java environment for PySpark.
"""
import os
import sys
import subprocess
from pathlib import Path

def check_java():
    """Check if Java is installed and configured."""
    # Check JAVA_HOME
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"JAVA_HOME is set to: {java_home}")
        if not Path(java_home).exists():
            print("Warning: JAVA_HOME path does not exist!")
    else:
        print("JAVA_HOME is not set")

    # Try to run java -version
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True, 
                              stderr=subprocess.STDOUT)
        if result.returncode == 0:
            print("\nJava is installed:")
            print(result.stdout)
        else:
            print("\nJava command failed:")
            print(result.stdout)
    except FileNotFoundError:
        print("\nJava is not found in PATH")

def setup_instructions():
    """Print manual setup instructions."""
    print("""
Java Setup Instructions for PySpark:

1. Download OpenJDK 11 LTS from Microsoft:
   https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-11

2. Extract the downloaded ZIP to C:\\Java\\jdk-11
   
3. Set JAVA_HOME environment variable:
   - Open System Properties (Win + R, type 'sysdm.cpl')
   - Click 'Environment Variables'
   - Under 'System Variables', click 'New'
   - Variable name: JAVA_HOME
   - Variable value: C:\\Java\\jdk-11

4. Add Java to PATH:
   - In System Variables, find 'Path'
   - Click 'Edit'
   - Click 'New'
   - Add: %JAVA_HOME%\\bin

5. Verify setup:
   - Open a new PowerShell window
   - Run: java -version
   - Run: echo $env:JAVA_HOME

Need the complete JDK download URL? Run this script with --url
""")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--url':
        print("\nMicrosoft OpenJDK 11 Download URL:")
        print("https://aka.ms/download-jdk/microsoft-jdk-11-windows-x64.zip")
        return

    print("Checking Java installation status...")
    print("-" * 40)
    check_java()
    print("\n" + "-" * 40)
    setup_instructions()

if __name__ == "__main__":
    main()
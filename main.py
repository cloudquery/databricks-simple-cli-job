#!/usr/bin/env python3
"""
Script to download cloudquery binary, set up environment, and run sync command.
"""

import os
import sys
import platform
import subprocess
import urllib.request
import stat
import argparse
from pyspark.dbutils import DBUtils

def detect_os():
    """Detect the operating system and architecture."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == "darwin":
        if machine in ["arm64", "aarch64"]:
            return "darwin_arm64"
        else:
            return "darwin_amd64"
    elif system == "linux":
        if machine in ["x86_64", "amd64"]:
            return "linux_amd64"
        else:
            return "linux_amd64"  # Default to amd64 for linux
    else:
        raise ValueError(f"Unsupported operating system: {system}")


def download_binary(os_type):
    """Download the cloudquery binary for the specified OS."""
    base_url = "https://github.com/cloudquery/cloudquery/releases/download/cli-v6.24.0"
    binary_name = f"cloudquery_{os_type}"
    url = f"{base_url}/{binary_name}"
    
    print(f"Downloading {binary_name} from {url}")
    
    try:
        urllib.request.urlretrieve(url, binary_name)
        print(f"Successfully downloaded {binary_name}")
        
        # Make the binary executable
        os.chmod(binary_name, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        print(f"Made {binary_name} executable")
        
        return binary_name
    except Exception as e:
        print(f"Error downloading binary: {e}")
        sys.exit(1)


def load_environment():
    """Load environment variables from .env file if it exists."""
    dbutils = DBUtils(spark)

    os.environ["AWS_ACCESS_KEY_ID"] = dbutils.secrets.get(scope="mariano", key="AWS_ACCESS_KEY_ID")
    os.environ["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get(scope="mariano", key="AWS_SECRET_ACCESS_KEY")
    os.environ["AWS_SESSION_TOKEN"] = dbutils.secrets.get(scope="mariano", key="AWS_SESSION_TOKEN")

    os.environ["CLOUDQUERY_API_KEY"] = dbutils.secrets.get(scope="mariano", key="CLOUDQUERY_API_KEY")

    os.environ["DATABRICKS_CATALOG"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_CATALOG")
    os.environ["DATABRICKS_SCHEMA"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_SCHEMA")
    os.environ["DATABRICKS_STAGING_PATH"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_STAGING_PATH")
    os.environ["DATABRICKS_HTTP_PATH"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_HTTP_PATH")
    os.environ["DATABRICKS_HOSTNAME"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_HOSTNAME")
    os.environ["DATABRICKS_ACCESS_TOKEN"] = dbutils.secrets.get(scope="mariano", key="DATABRICKS_ACCESS_TOKEN")


def expand_env_vars_in_yaml(yaml_file):
    """Expand environment variables in the YAML file."""
    if not os.path.exists(yaml_file):
        print(f"Error: {yaml_file} not found")
        sys.exit(1)
    
    with open(yaml_file, 'r') as f:
        content = f.read()
    
    # Expand environment variables
    expanded_content = os.path.expandvars(content)
    
    # Write the expanded content back to a temporary file
    temp_yaml = f"{yaml_file}.expanded"
    with open(temp_yaml, 'w') as f:
        f.write(expanded_content)
    
    print(f"Expanded environment variables in {yaml_file} -> {temp_yaml}")
    return temp_yaml


def run_cloudquery_sync(binary_path, yaml_file):
    """Run the cloudquery sync command."""
    cmd = [f"./{binary_path}", "sync", yaml_file]
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print("Cloudquery sync completed successfully")
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running cloudquery sync: {e}")
        sys.exit(1)


def cleanup_temp_file(temp_file):
    """Clean up temporary files."""
    try:
        os.remove(temp_file)
        print(f"Cleaned up temporary file: {temp_file}")
    except OSError as e:
        print(f"Warning: Could not remove temporary file {temp_file}: {e}")


def main():
    """Main function to orchestrate the entire process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run cloudquery sync with specified YAML file')
    parser.add_argument('yaml_file', help='Path to the YAML configuration file')
    args = parser.parse_args()
    
    print("Starting cloudquery setup and execution...")
    
    # Step 1: Load environment variables
    load_environment()
    
    # Step 2: Detect OS and download appropriate binary
    os_type = detect_os()
    print(f"Detected OS type: {os_type}")
    
    binary_name = download_binary(os_type)
    
    # Step 3: Expand environment variables in YAML file
    yaml_file = args.yaml_file
    expanded_yaml = expand_env_vars_in_yaml(yaml_file)
    
    try:
        # Step 4: Run cloudquery sync
        run_cloudquery_sync(binary_name, expanded_yaml)
    finally:
        # Step 5: Cleanup
        cleanup_temp_file(expanded_yaml)
    
    print("Script completed successfully!")


if __name__ == "__main__":
    main()
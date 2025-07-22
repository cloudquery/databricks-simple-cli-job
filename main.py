#!/usr/bin/env python3
"""
Script to download cloudquery binary, set up environment, and run sync command.
"""

import os
import sys
import subprocess
from pyspark.dbutils import DBUtils

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


def run_cloudquery_sync(yaml_file):
    """Run the cloudquery sync command."""
    cmd = [f"python", "-m", "cloudquery", "sync", yaml_file]
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print("Cloudquery sync completed successfully")
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running cloudquery sync: {e}")
        sys.exit(1)

def main():
    """Main function to orchestrate the entire process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run cloudquery sync with specified YAML file')
    parser.add_argument('yaml_file', help='Path to the YAML configuration file')
    args = parser.parse_args()

    load_environment()
    
    yaml_file = args.yaml_file
    expanded_yaml = expand_env_vars_in_yaml(yaml_file)
    
    run_cloudquery_sync(expanded_yaml)
    print("Script completed successfully!")

if __name__ == "__main__":
    main()
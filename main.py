#!/usr/bin/env python3
"""
Script to download cloudquery binary, set up environment, and run sync command.
"""

import argparse
import os
import sys
import subprocess
from pyspark.dbutils import DBUtils

def load_environment(scope):
    """Load environment variables from .env file if it exists."""
    dbutils = DBUtils(spark)
    metas = dbutils.secrets.list(scope=scope)
    for meta in metas:
        os.environ[meta.key] = dbutils.secrets.get(scope=scope, key=meta.key)

def expand_env_vars_in_yaml(yaml_file):
    """Expand environment variables in the YAML file."""
    if not os.path.exists(yaml_file):
        print(f"Error: {yaml_file} not found")
        sys.exit(1)
    
    with open(yaml_file, 'r') as f:
        content = f.read()
    
    expanded_content = os.path.expandvars(content)
    temp_yaml = f"/tmp/spec.expanded"
    with open(temp_yaml, 'w') as f:
        f.write(expanded_content)
    
    print(f"Expanded environment variables")
    return temp_yaml

def main():
    """Main function to orchestrate the entire process."""
    parser = argparse.ArgumentParser(description='Run cloudquery sync with specified YAML file')
    parser.add_argument('--spec', required=True, help='Path to the YAML configuration file')
    parser.add_argument('--scope', required=True, help='Scope to load Databricks secrets from')
    args = parser.parse_args()

    load_environment(args.scope)
    expand_env_vars_in_yaml(args.spec)
    
    os.chdir('/tmp')

    subprocess.run(["python", "-m", "cloudquery", "sync", "/tmp/spec.expanded"], check=True, capture_output=False)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Script to download cloudquery binary, set up environment, and run sync command.
"""

import argparse
import os
import subprocess
from pyspark.dbutils import DBUtils

def load_environment(scope):
    """Expose secrets from Databricks secrets scope to environment variables."""
    dbutils = DBUtils(spark)
    metas = dbutils.secrets.list(scope=scope)
    for meta in metas:
        os.environ[meta.key] = dbutils.secrets.get(scope=scope, key=meta.key)

def main():
    """Main function to orchestrate the entire process."""
    parser = argparse.ArgumentParser(description='Run cloudquery sync with specified YAML file')
    parser.add_argument('--spec', required=True, help='Path to the YAML configuration file')
    parser.add_argument('--secrets-scope', required=True, help='Scope to load Databricks secrets from')
    args = parser.parse_args()

    load_environment(args.secrets_scope)
    
    os.chdir('/tmp')

    subprocess.run(["python", "-m", "cloudquery", "sync", "/tmp/spec.expanded"], check=True, capture_output=False)

if __name__ == "__main__":
    main()
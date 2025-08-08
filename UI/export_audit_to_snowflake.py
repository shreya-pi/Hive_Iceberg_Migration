# export_audit_to_snowflake.py
import os
import sys
import argparse
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from config import SNOWFLAKE_CONFIG

# Load environment variables for credentials
load_dotenv()

def main(csv_file_path, sf_db, sf_schema):
    """
    Reads the audit CSV and uploads its content to a Snowflake table.
    """
    target_table = "AUDIT_TABLE"
    print(f"--- Starting Export of '{csv_file_path}' to Snowflake ---")
    print(f"Target: {sf_db}.{sf_schema}.{target_table}")

    # 1. Read the source CSV into a Pandas DataFrame
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Successfully read {len(df)} rows from {csv_file_path}.")
    except Exception as e:
        print(f"FATAL: Could not read the source CSV file. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("Successfully connected to Snowflake.")
    except Exception as e:
        print(f"FATAL: Could not connect to Snowflake. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Write the DataFrame to Snowflake
    try:
        print(f"Writing data to Snowflake table '{target_table}'...")
        # write_pandas will automatically handle CREATE OR REPLACE logic.
        # It's highly efficient for uploading data.
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name=target_table.upper(), # Snowflake table names are often uppercase
            database=sf_db.upper(),
            schema=sf_schema.upper(),
            auto_create_table=True, # Create the table if it doesn't exist
            overwrite=True # Replace the table content if it exists
        )
        if success:
            print(f"SUCCESS: Wrote {nrows} rows in {nchunks} chunk(s) to '{target_table}'.")
        else:
            print(f"ERROR: Snowflake write_pandas command reported a failure.", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while writing to Snowflake. Details: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()
        print("Snowflake connection closed.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Export audit CSV data to Snowflake.")
    parser.add_argument("--csv-path", required=True, help="Path to the audit CSV file.")
    parser.add_argument("--sf-db", required=True, help="Target database in Snowflake.")
    parser.add_argument("--sf-schema", required=True, help="Target schema in Snowflake.")
    args = parser.parse_args()
    main(args.csv_path, args.sf_db, args.sf_schema)
# register_on_snowflake.py
import os
import sys
import json
import re
import argparse
from pyspark.sql import SparkSession
import snowflake.connector
from dotenv import load_dotenv
from config import SNOWFLAKE_CONFIG  # Ensure this is defined in your config module

load_dotenv()  # Load environment variables if needed



def get_current_metadata_location(spark, full_iceberg_table_name):
    """
    Robustly gets the relative path to the latest metadata file for an Iceberg table.
    This version correctly calculates the path relative to the warehouse root.
    """
    try:
        print(f"Describing table to find its root location: {full_iceberg_table_name}")
        
        result_df = spark.sql(f"DESCRIBE EXTENDED {full_iceberg_table_name}")
        location_row = result_df.filter("col_name = 'Location'").first()
        
        if not location_row:
            print(f"ERROR: Could not determine root 'Location' for table {full_iceberg_table_name}.", file=sys.stderr)
            return None
        
        table_root_location = location_row[1]
        metadata_dir_path = os.path.join(table_root_location, "metadata")
        
        # --- List files to find the latest version ---
        URI = spark._jvm.java.net.URI
        Path = spark._jvm.org.apache.hadoop.fs.Path
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        fs = FileSystem.get(URI(metadata_dir_path), spark._jsc.hadoopConfiguration())
        file_statuses = fs.listStatus(Path(metadata_dir_path))
        
        highest_version = -1
        latest_metadata_filename = None
        for status in file_statuses:
            filename = status.getPath().getName()
            match = re.match(r"v(\d+)\.metadata\.json", filename)
            if match:
                version = int(match.group(1))
                if version > highest_version:
                    highest_version = version
                    latest_metadata_filename = filename
                    
        if not latest_metadata_filename:
            print(f"ERROR: No 'vN.metadata.json' files found in {metadata_dir_path}", file=sys.stderr)
            return None
            
        full_metadata_path = os.path.join(metadata_dir_path, latest_metadata_filename)
        print(f"Found full metadata path: {full_metadata_path}")
        
        # --- THIS IS THE CORRECTED PATH LOGIC ---
        # 1. Get the warehouse location from the Spark config.
        warehouse_location = spark.conf.get("spark.sql.catalog.azure_blob_catalog.warehouse")
        
        # 2. Ensure both paths have a consistent trailing slash for replacement.
        if not warehouse_location.endswith('/'):
            warehouse_location += '/'
        
        # 3. Create the relative path by removing the warehouse prefix.
        if full_metadata_path.startswith(warehouse_location):
            relative_path = full_metadata_path.replace(warehouse_location, '', 1)
            print(f"Derived relative path for Snowflake: {relative_path}")
            return relative_path
        else:
            print(f"ERROR: Full metadata path '{full_metadata_path}' does not start with warehouse location '{warehouse_location}'. Cannot create relative path.", file=sys.stderr)
            return None

    except Exception as e:
        print(f"ERROR: An unexpected error occurred while determining metadata location for {full_iceberg_table_name}. Details: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return None




def main(azure_db, tables, sf_db, sf_schema):
    """
    Main logic to register Azure-based Iceberg tables in Snowflake.
    """
    print("--- Starting Snowflake Table Registration ---")
    
    # 1. Initialize a lightweight Spark session to talk to the Azure Iceberg catalog
    # The configuration here MUST match the one used by the worker for migration.
    spark = SparkSession.builder \
        .appName("IcebergMetadataReaderForSnowflake") \
        .master("local[*]") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.azure_blob_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.azure_blob_catalog.type", "hadoop") \
        .config("spark.sql.catalog.azure_blob_catalog.uri", "thrift://localhost:9083") \
        .config("spark.sql.catalog.azure_blob_catalog.warehouse", "wasbs://data@tulapidemo.blob.core.windows.net/iceberg_warehouse/") \
        .config("spark.hadoop.fs.azure.account.key.tulapidemo.blob.core.windows.net", os.getenv("AZURE_KEY")) \
        .getOrCreate()

    # 2. Connect to Snowflake
    # It's best practice to use environment variables for credentials
    try:
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("Successfully connected to Snowflake.")
    except Exception as e:
        print(f"FATAL: Could not connect to Snowflake. Error: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    cursor = sf_conn.cursor()

    # 3. Loop through tables, get metadata path, and create table in Snowflake
    for table in tables:
        full_iceberg_name = f"azure_blob_catalog.{azure_db}.{table}"
        print(f"\nProcessing table: {full_iceberg_name}")
        
        metadata_path = get_current_metadata_location(spark, full_iceberg_name)
        
        if not metadata_path:
            print(f"Skipping table {table} due to metadata read failure.")
            continue

        # These are your Snowflake integration/volume names
        catalog_integration = 'my_hive_catalog_integration'
        external_volume = 'iceberg_azure_vol'
        
        # This is the Snowflake table name, which we can keep the same as the source
        snowflake_table_name = table

        # Construct the final, robust SQL command
        sql_command = f"""
        CREATE OR REPLACE ICEBERG TABLE {sf_db}.{sf_schema}.{snowflake_table_name}
          CATALOG = '{catalog_integration}'
          EXTERNAL_VOLUME = '{external_volume}'
          METADATA_FILE_PATH = '{metadata_path}';
        """
        
        print(f"Executing Snowflake DDL for table {snowflake_table_name}:")
        print(sql_command)
        
        try:
            cursor.execute(sql_command)
            print(f"SUCCESS: Table '{snowflake_table_name}' created/replaced in Snowflake.")
        except Exception as e:
            print(f"ERROR: Snowflake DDL execution failed for table '{snowflake_table_name}'. Details: {e}", file=sys.stderr)
            
    cursor.close()
    sf_conn.close()
    spark.stop()
    print("\n--- Snowflake Table Registration Finished ---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Register Azure Iceberg tables in Snowflake.")
    parser.add_argument("--azure-db", required=True, help="Source database in the azure_blob_catalog.")
    parser.add_argument("--tables", required=True, nargs='+', help="Tables to register.")
    parser.add_argument("--sf-db", required=True, help="Target database in Snowflake.")
    parser.add_argument("--sf-schema", required=True, help="Target schema in Snowflake.")
    args = parser.parse_args()
    
    main(args.azure_db, args.tables, args.sf_db, args.sf_schema)
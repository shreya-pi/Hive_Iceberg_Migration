# non_interactive_iceberg_ingest.py
# (This is the target script. It is stable and never gets modified.)

import sys
import argparse
from pyspark.sql import SparkSession

def main(database_name, table_names):
    # ... (The full logic from the previous answer)
    # This function expects database_name and table_names as arguments.
    print(f"Executing Spark job for DB: '{database_name}', Tables: {table_names}")
    # ... your Spark logic here ...
    spark = SparkSession.builder \
        .appName(f"IcebergIngestion-{database_name}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive_catalog.type", "hive") \
        .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    target_namespace = f"{database_name}_iceberg"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_catalog.{target_namespace}")

    print("\n--- Starting Table Migration ---")
    for table in table_names:
        try:
            source_table_name = f"{database_name}.{table}"
            target_table_name = f"hive_catalog.{target_namespace}.{table}"
            print(f"\nProcessing '{source_table_name}' -> '{target_table_name}'...")
            spark.table(source_table_name)\
                 .writeTo(target_table_name)\
                 .using("iceberg")\
                 .createOrReplace()
            print(f"  SUCCESS: Wrote data to '{target_table_name}'.")
        except Exception as e:
            print(f"  ERROR processing table '{table}': {e}", file=sys.stderr)
    
    spark.stop()
    print("Spark job finished.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Migrate Hive tables to Iceberg.")
    parser.add_argument("--database", required=True, type=str)
    parser.add_argument("--tables", required=True, nargs='+', type=str)
    args = parser.parse_args()
    main(database_name=args.database, table_names=args.tables)
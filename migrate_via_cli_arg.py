# non_interactive_migrator.py
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def main(source_db, dest_db, table_names):
    """
    Main migration logic. Migrates one or more tables from the hive_catalog
    to the azure_blob_catalog based on provided arguments.
    """
    # The crucial catalog configurations must be provided in the spark-submit command
    spark = SparkSession.builder.appName("IcebergCrossCatalogMigration").getOrCreate()

    print(f"--- Starting migration from 'hive_catalog.{source_db}' to 'azure_blob_catalog.{dest_db}' ---")

    # Ensure the destination database/namespace exists in the target catalog
    try:
        print(f"Ensuring destination database 'azure_blob_catalog.{dest_db}' exists...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS azure_blob_catalog.{dest_db}")
    except Exception as e:
        print(f"FATAL: Could not create destination database. Error: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    successful_migrations = []
    failed_migrations = []

    for table in table_names:
        source_table_name = f"hive_catalog.{source_db}.{table}"
        destination_table_name = f"azure_blob_catalog.{dest_db}.{table}"

        print("\n" + "="*50)
        print(f"Processing: {source_table_name}  ==>  {destination_table_name}")
        print("="*50)

        try:
            # 1. Read the source table
            print(f"Reading from source: {source_table_name}")
            source_df = spark.read.table(source_table_name)
            
            # Optional: Show schema for verification
            # source_df.printSchema()

            # 2. Write to the destination. Use createOrReplace() to make the job idempotent.
            print(f"Writing to destination: {destination_table_name}")
            source_df.writeTo(destination_table_name).createOrReplace()

            print(f"SUCCESS: Migration for table '{table}' complete!")
            successful_migrations.append(table)

        except AnalysisException as e:
            print(f"ERROR: Could not find source table '{source_table_name}'. Please check spelling and permissions.", file=sys.stderr)
            print(f"DETAILS: {e}", file=sys.stderr)
            failed_migrations.append(table)
        except Exception as e:
            print(f"ERROR: An unexpected error occurred while migrating table '{table}'.", file=sys.stderr)
            print(f"DETAILS: {e}", file=sys.stderr)
            failed_migrations.append(table)
    
    print("\n--- Migration Summary ---")
    print(f"Successfully migrated: {len(successful_migrations)} tables ({', '.join(successful_migrations)})")
    if failed_migrations:
        print(f"Failed to migrate: {len(failed_migrations)} tables ({', '.join(failed_migrations)})")
    else:
        print("All selected tables migrated without errors.")

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Migrate Iceberg tables from Hive catalog to Azure Blob catalog.")
    
    parser.add_argument("--source-db", required=True, type=str,
                        help="The source database/namespace in 'hive_catalog'.")
    
    parser.add_argument("--dest-db", required=True, type=str,
                        help="The destination database/namespace in 'azure_blob_catalog'.")

    parser.add_argument("--tables", required=True, nargs='+', type=str,
                        help="A space-separated list of table names to migrate.")
                        
    args = parser.parse_args()
    
    main(source_db=args.source_db, dest_db=args.dest_db, table_names=args.tables)
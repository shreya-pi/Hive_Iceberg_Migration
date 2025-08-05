# interactive_iceberg_ingest_v2.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def get_user_selection(prompt, options):
    """
    Generic function to display options and get a valid user selection.
    Returns the index of the selected option.
    """
    print(f"\n--- {prompt} ---")
    for i, option in enumerate(options):
        print(f"[{i+1}] {option}")

    choice = -1
    while choice < 1 or choice > len(options):
        try:
            input_str = input(f"Select an option (1-{len(options)}): ")
            choice = int(input_str)
            if not (1 <= choice <= len(options)):
                print("Invalid choice. Please select a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")
    return choice - 1 # Return the 0-based index



def get_multiple_user_selections(prompt, options):
    print(f"\n--- {prompt} ---")
    for i, option in enumerate(options):
        print(f"[{i+1}] {option}")
    selected_options = []
    while not selected_options:
        try:
            input_str = input(f"Select one or more options (e.g., 1,3,5 or '*' for all): ")
            if input_str.strip() == '*':
                return options
            indices = [int(i.strip()) - 1 for i in input_str.split(',')]
            valid_indices = [i for i in indices if 0 <= i < len(options)]
            if len(valid_indices) != len(indices):
                print(f"Warning: Some invalid selections were ignored.")
            if not valid_indices:
                print("No valid options selected. Please try again.")
                continue
            selected_options = list(dict.fromkeys([options[i] for i in valid_indices]))
        except ValueError:
            print("Invalid input. Please enter comma-separated numbers (e.g., 1,3,5) or '*'.")
    return selected_options

def main():
    """
    Main function to run the interactive ingestion process.
    """
    print("Initializing SparkSession with Iceberg extensions...")

    try:
        spark = SparkSession.builder \
            .appName("InteractiveIcebergIngestion") \
            .master("local[*]") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.hive_catalog.type", "hive") \
            .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        print("SparkSession created successfully.")
    except Exception as e:
        print(f"FATAL: Could not create SparkSession. Error: {e}")
        sys.exit(1)

    try:
        # --- INTERACTIVE DATABASE SELECTION ---
        db_df = spark.sql("SHOW DATABASES")
        # To see the column name in your environment, you can uncomment the next line:
        # db_df.show()
        databases = [row[0] for row in db_df.collect()] ## CHANGED ##
        if not databases:
            print("No databases found in Hive Metastore. Exiting.")
            return

        selected_db_index = get_user_selection("Available Databases", databases)
        selected_database = databases[selected_db_index]
        print(f"\n=> You selected database: '{selected_database}'")

        # --- INTERACTIVE TABLE SELECTION ---
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {selected_database}")
            # The schema is (database, tableName, isTemporary)
            # To see it, you can uncomment the next line:
            # tables_df.show()
            tables = [row[1] for row in tables_df.collect() if not row[2]] ## CHANGED ##
        except AnalysisException:
             tables = [] # Handle case where database has no tables

        if not tables:
            print(f"No tables found in database '{selected_database}'. Exiting.")
            return

        selected_tables = get_multiple_user_selections(f"Tables in '{selected_database}'", tables)
        print("\n=> You selected the following tables to migrate:")
        for tbl in selected_tables:
            print(f"   - {tbl}")

        # --- DYNAMIC PROCESSING LOOP ---
        target_namespace = f"{selected_database}_iceberg"
        print(f"\nTarget Iceberg namespace will be: 'hive_catalog.{target_namespace}'")
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_catalog.{target_namespace}")
        print(f"Namespace 'hive_catalog.{target_namespace}' created or already exists.")

        print("\n--- Starting Table Migration ---")
        for table in selected_tables:
            try:
                source_table_name = f"{selected_database}.{table}"
                hiveDf = spark.table(source_table_name)
                
                target_table_name = f"hive_catalog.{target_namespace}.{table}"

                print(f"\nProcessing '{source_table_name}' -> '{target_table_name}'...")
                
                hiveDf.writeTo(target_table_name).using("iceberg").createOrReplace()
                print(f"  SUCCESS: Wrote data to '{target_table_name}'.")
                
                print(f"  Verifying by showing top 5 rows from new Iceberg table:")
                spark.table(target_table_name).show(5, False)

            except Exception as e:
                print(f"  ERROR processing table '{table}': {e}")
                print(f"  Skipping this table and continuing...")

        print("\n--- Migration Process Finished ---")
        print(f"Final list of tables in 'hive_catalog.{target_namespace}':")
        spark.sql(f"SHOW TABLES IN hive_catalog.{target_namespace}").show(truncate=False)

    except Exception as e:
        print(f"\nAn unexpected error occurred during the process: {e}")
    finally:
        print("Done! Stopping SparkSession.")
        spark.stop()

if __name__ == '__main__':
    main()
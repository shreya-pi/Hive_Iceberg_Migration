# interactive_migrator_launcher.py
import subprocess
import sys
from pyspark.sql import SparkSession

# --- Helper functions for user input (same as before) ---
def get_user_selection(prompt, options):
    print(f"\n--- {prompt} ---")
    for i, option in enumerate(options):
        print(f"[{i+1}] {option}")
    choice = -1
    while not (1 <= choice <= len(options)):
        try:
            choice = int(input(f"Select an option (1-{len(options)}): "))
        except ValueError:
            print("Invalid input. Please enter a number.")
    return options[choice - 1]

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
            selected_options = [options[i] for i in indices if 0 <= i < len(options)]
            if not selected_options:
                print("No valid options selected. Please try again.")
        except (ValueError, IndexError):
            print("Invalid input. Please enter comma-separated numbers from the list.")
    return list(dict.fromkeys(selected_options))


def main():
    print("--- Interactive Iceberg Cross-Catalog Migration Launcher ---")
    print("Connecting to 'hive_catalog' to fetch metadata...")

    # A local SparkSession to get the list of databases and tables from the source catalog
    try:
        spark = SparkSession.builder \
            .appName("MetadataFetcher") \
            .master("local[*]") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.hive_catalog.type", "hive") \
            .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083") \
            .getOrCreate()
    except Exception as e:
        print(f"FATAL: Could not create a local SparkSession to fetch metadata. Error: {e}", file=sys.stderr)
        sys.exit(1)
        
    # --- Gather user input ---
    databases = [row[0] for row in spark.sql("SHOW DATABASES IN hive_catalog").collect()]
    selected_db = get_user_selection("Select a source database from 'hive_catalog'", databases)

    tables = [row[1] for row in spark.sql(f"SHOW TABLES IN hive_catalog.{selected_db}").collect()]
    if not tables:
        print(f"No tables found in 'hive_catalog.{selected_db}'. Exiting.")
        spark.stop()
        return
        
    selected_tables = get_multiple_user_selections(f"Tables in 'hive_catalog.{selected_db}'", tables)
    
    # Ask for the destination database name
    dest_db_name = input(f"\nEnter the destination database name in 'azure_blob_catalog' (default: {selected_db}): ")
    if not dest_db_name:
        dest_db_name = selected_db # Use the same name as source if input is empty

    spark.stop() # We are done with the local spark session
    print("\nMetadata collection complete. Ready to launch the main Spark job.")

    # --- Build the spark-submit command ---
    # NOTE: You MUST add your Azure packages and configurations here.
    spark_submit_command = [
        "spark-submit",
        "--deploy-mode", "client",
        "--conf", "spark.hadoop.fs.defaultFS=wasbs://data@tulapidemo.blob.core.windows.net/",
        "--conf", "spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.hive_catalog.type=hive",
        "--conf", "spark.sql.catalog.hive_catalog.uri=thrift://localhost:9083",
        "--conf", "spark.sql.catalog.azure_blob_catalog=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.azure_blob_catalog.type=hadoop",
        "--conf", "spark.sql.catalog.azure_blob_catalog.warehouse=wasbs://data@tulapidemo.blob.core.windows.net/iceberg_warehouse/",
        "--conf", "spark.hadoop.fs.azure.impl=org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        "--conf", "spark.hadoop.fs.azure.account.key.tulapidemo.blob.core.windows.net=${AZURE_KEY}",
        "--conf", "spark.hadoop.fs.azure.header.x-ms-blob-type=BlockBlob",
        "--conf", "spark.hadoop.fs.azure.atomic.rename.dir=/tmp/hdfs-over-wasb-rename-staging",
        "--conf", "spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization=true",
        "spark_scripts/migrate_table_1.py",  # The name of our "engine" script
        "--source-db", selected_db,
        "--dest-db", dest_db_name,
        "--tables"
    ]

    spark_submit_command.extend(selected_tables)

    print("\nExecuting the following command (review your configurations):")
    print(" ".join(spark_submit_command))
    print("-" * 60)

    # --- Execute the command ---
    process = subprocess.Popen(spark_submit_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            
    rc = process.poll()
    if rc == 0:
        print("-" * 60, "\nSpark job finished successfully.")
    else:
        print("-" * 60, f"\nERROR: Spark job failed with exit code {rc}.")

if __name__ == "__main__":
    main()
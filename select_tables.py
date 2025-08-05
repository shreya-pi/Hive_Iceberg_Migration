import subprocess
import sys
from pyspark.sql import SparkSession

# --- Helper functions for user input (copied for convenience) ---
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
    return list(dict.fromkeys(selected_options)) # Return unique selections

def main():
    """
    Interactively gathers info from the user, then constructs and runs
    the spark-submit command.
    """
    print("Starting interactive launcher...")
    print("Connecting to Spark locally to fetch metadata...")

    # Use a minimal, local SparkSession just to get DB and table names.
    # This is lightweight and doesn't need all the Iceberg configs.
    try:
        spark = SparkSession.builder \
            .appName("MetadataFetcher") \
            .master("local[*]") \
            .config("spark.sql.catalogImplementation", "hive") \         
            .enableHiveSupport() \
            .getOrCreate()
    except Exception as e:
        print(f"FATAL: Could not create a local SparkSession to fetch metadata. Error: {e}")
        sys.exit(1)

    # --- Gather user input ---
    databases = [row[0] for row in spark.sql("SHOW DATABASES").collect()]
    selected_db = get_user_selection("Select a source database", databases)

    tables = [row[1] for row in spark.sql(f"SHOW TABLES IN {selected_db}").collect()]
    if not tables:
        print(f"No tables found in database '{selected_db}'. Exiting.")
        spark.stop()
        return

    selected_tables = get_multiple_user_selections(f"Tables in '{selected_db}'", tables)

    spark.stop() # We are done with the local spark session
    print("\nMetadata collection complete. Ready to launch the main Spark job.")

    # --- Build the spark-submit command ---
    # Using a list is safer than a single string to avoid shell injection
    spark_submit_command = [
        "spark-submit",
        "--deploy-mode", "client",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.7.1",
        "--jars", "$(echo ~/spark/jars/*.jar | tr ' ' ',')",
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.hive_catalog.type=hive",
        "--conf", "spark.sql.catalog.hive_catalog.warehouse=hdfs://0.0.0.0:9000/user/hive/warehouse",
        "--conf", "spark.sql.hive.metastore.uris=thrift://localhost:9083",
        "--conf", "spark.sql.hive.metastore.jars.path=file:///home/hadoop/hive/lib/*",
        "spark_scripts/create_iceberg_table_2.py",
        "--database", selected_db,
        "--tables"
    ]

    # Add all selected tables to the command list
    spark_submit_command.extend(selected_tables)

    print("\nExecuting the following command:")
    # Print the command in a user-friendly way
    print(" ".join(spark_submit_command))
    print("-" * 40)

    # --- Execute the command ---
    # This will run the command and stream its output to your console.
    process = subprocess.Popen(spark_submit_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Read and print the output line by line in real-time
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())

    rc = process.poll()
    if rc == 0:
        print("-" * 40)
        print("Spark job finished successfully.")
    else:
        print("-" * 40)
        print(f"ERROR: Spark job failed with exit code {rc}.")


if __name__ == "__main__":
    main()

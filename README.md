# Iceberg Data Migration Toolkit

This repository contains a suite of Spark scripts designed to facilitate the ingestion of data into Apache Iceberg tables and the migration of those tables between different environments, such as an on-premises Hadoop cluster and a cloud provider like Microsoft Azure.

The toolkit is designed with a "Launcher/Engine" architecture for a robust and user-friendly experience.
*   **Engine Scripts**: Parameterized, non-interactive Python scripts that perform the core data-moving work. They are designed to be executed via `spark-submit` and are ideal for automation.
*   **Launcher Scripts**: Interactive Python scripts that provide a user-friendly command-line interface. They query for available databases and tables, ask the user for selections, and then automatically construct and execute the correct `spark-submit` command to run the appropriate engine.

## Core Workflows

This toolkit supports two primary, end-to-end workflows:

1.  **Workflow 1: Ingest from Hive to On-Prem Iceberg**
    *   **Goal**: Convert traditional Hive tables into new Iceberg tables within an on-premises Hive-based Iceberg catalog (`hive_catalog`).
    *   **Process**: An interactive launcher lists available Hive databases and tables. The user selects which ones to ingest, and the engine script performs the conversion.

2.  **Workflow 2: Migrate from On-Prem Iceberg to Azure Iceberg**
    *   **Goal**: Copy existing Iceberg tables from the on-premises catalog to a separate Iceberg catalog running on Azure Blob Storage (`azure_blob_catalog`).
    *   **Process**: A launcher script lists databases and tables from the on-prem Iceberg catalog. The user selects tables to migrate, and the engine script performs the cross-catalog copy.

---

## Prerequisites and Setup

Before using any scripts, ensure your environment is correctly configured.

### 1. Software Requirements
*   **Java**: 8 or 11
*   **Apache Spark**: 3.3.x or compatible
*   **Python**: 3.7+
*   **PySpark**: Must be installed in your Python environment (`pip install pyspark`).
*   **Hive Metastore**: An accessible Hive Metastore service for both on-prem and (optionally) cloud catalogs.

### 2. Environment Variables
It is recommended to have `SPARK_HOME` and `JAVA_HOME` set in your environment.

### 3. Spark Configuration (Crucial)
All `spark-submit` commands require specific packages and configurations for Iceberg and cloud connectivity. You must provide these at runtime.

**Example `spark-submit` packages and configs:**
```bash
spark-submit \
  # --- Required for Iceberg ---
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.13-1.7.1 \
  
  # --- Required for Azure Connectivity ---
  --packages org.apache.hadoop:hadoop-azure:3.3.6,com.azure:azure-storage-blob:8.6.6 \

  # --- Required for On-Prem Hive Catalog ---
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive_catalog.type=hive \
  --conf spark.sql.catalog.hive_catalog.uri=thrift://<ON-PREM-METASTORE-IP>:9083 \
  
  # --- Required for Azure Blob Catalog ---
  --conf spark.sql.catalog.azure_blob_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.azure_blob_catalog.type=hive \
  --conf spark.hadoop.fs.azure.account.key.<your-storage-account>.dfs.core.windows.net=<YOUR-AZURE-STORAGE-KEY> \
  
  <your-script-name.py> [ARGUMENTS]
```
**Note:** The interactive launcher scripts will automatically include these configurations when they build the `spark-submit` command. You must **edit the launcher scripts** to include your specific URIs, account names, and secret keys.

---

## File Breakdown and Usage

Here is a detailed explanation of each file in this toolkit.

| File Name                              | Purpose                                                                                                      | How to Run / Usage Example                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| **Workflow 1: Hive -> On-Prem Iceberg**|                                                                                                              |                                                                                                                             |
| `select_tables.py`                     | **(Launcher)** Interactively prompts the user to select a Hive database and tables for ingestion into Iceberg. | Run directly with Python: <br> `python3 select_tables.py`                                                                   |
| `create_multiple_icerberg_tables_hive.py` | **(Engine)** The non-interactive script that reads source Hive tables and writes them as new Iceberg tables. | Called automatically by the launcher. Can be run directly for automation: <br> `spark-submit ... create_multiple_icerberg_tables_hive.py --database default --tables table1 table2` |
| **Workflow 2: On-Prem -> Azure**       |                                                                                                              |                                                                                                                             |
| `select_migrate_tables.py`             | **(Launcher)** Interactively prompts the user to select tables from the on-prem Iceberg catalog for migration. | Run directly with Python: <br> `python3 select_migrate_tables.py`                                                          |
| `migrate_via_cli_arg.py`               | **(Engine)** The non-interactive script that migrates Iceberg tables from one catalog to another.            | Called automatically by the launcher. For automation: <br> `spark-submit ... migrate_via_cli_arg.py --source-db itest --dest-db itest --tables table1` |
| **Legacy/Reference Scripts**           |                                                                                                              |                                                                                                                             |
| `create_iceberg_from_hive.scala`       | A reference Scala script showing how to convert a single, hardcoded Hive table to Iceberg.                     | Run via `spark-shell` by pasting the code.                                                                                  |
| `create_iceberg_from_hdfs.scala`       | A reference Scala script showing how to create an Iceberg table from files on HDFS (e.g., Parquet).            | Run via `spark-shell` by pasting the code.                                                                                  |
| `migrate_iceberg_to_Azure.py`          | A reference Python script showing the basic logic for migrating a single, hardcoded Iceberg table.             | Run via `spark-submit`.                                                                                                     |
| `create_multiple_iceberg_command_line.py` | An older or alternative version of the Hive-to-Iceberg engine script.                                      | Usage is similar to `create_multiple_icerberg_tables_hive.py`. It is recommended to consolidate into one engine.           |


## Detailed Workflow Walkthroughs

### Workflow 1: Ingesting from Hive to On-Prem Iceberg

This process converts your existing Hive tables into modern Iceberg tables within the same data warehouse environment.

1.  **Launch the Interactive Script**:
    ```bash
    # Ensure pyspark is in your environment
    python3 select_tables.py
    ```
2.  **Select a Database**: The script will connect to your Hive Metastore, list all available databases, and prompt you to select one.
3.  **Select Tables**: After you select a database, the script will list all tables within it. You can select one, multiple (e.g., `1,3,5`), or all (`*`) tables to ingest.
4.  **Automatic Execution**: The launcher script will then build and execute a `spark-submit` command, calling `create_multiple_icerberg_tables_hive.py` with your selections as command-line arguments.
5.  **Monitor the Job**: You will see the live output of the Spark job in your terminal. For each selected table, it will create a corresponding Iceberg table in a new database named `<source_db_name>_iceberg`.

### Workflow 2: Migrating On-Prem Iceberg Tables to Azure

This process copies fully structured Iceberg tables from your on-premises catalog to a cloud-based catalog, enabling a hybrid-cloud strategy.

1.  **Configure the Launcher**: **This is a mandatory one-time step.** Open `select_migrate_tables.py` in a text editor. Find the `spark_submit_command` list and fill in all your Azure-specific configurations (`--packages`, `--conf` for `azure_blob_catalog`, and especially your Azure Storage account key).
2.  **Launch the Interactive Script**:
    ```bash
    python3 select_migrate_tables.py
    ```
3.  **Select a Source Database**: The script will connect to your on-prem `hive_catalog`, list all databases (namespaces), and prompt you for a selection.
4.  **Select Tables for Migration**: It will list all Iceberg tables in the selected database and ask you to choose which ones to migrate.
5.  **Specify Destination Database**: You will be asked to name the destination database in the `azure_blob_catalog`. You can press Enter to keep the same name as the source.
6.  **Automatic Execution**: The launcher will build the complex `spark-submit` command (including all the Azure credentials you configured) and run the `migrate_via_cli_arg.py` engine.
7.  **Monitor the Job**: The Spark job will read each table from the on-prem source and write it to the Azure destination. You will see its progress in your terminal.
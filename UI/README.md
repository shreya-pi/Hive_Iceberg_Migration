
# Data Operations Portal

This repository contains a comprehensive, UI-driven toolkit for managing data workflows between traditional Hive, Apache Iceberg, Microsoft Azure, and Snowflake. The application is built around a robust, decoupled architecture featuring a Streamlit web interface and a backend job processing system.

## Core Architecture

The system is designed with a "Launcher/Engine/Worker" architecture for maximum modularity, scalability, and user-friendliness.

-   **Streamlit UI (`app.py`)**: A centralized, interactive web interface where users can trigger complex data operations without writing any code or using the command line. It acts as the "Launcher".
-   **Backend Worker (`worker.py`)**: A long-running Python process that continuously monitors a job queue. It is responsible for picking up job requests submitted from the UI and executing the appropriate engine script.
-   **Engine Scripts (`*.py`)**: A collection of non-interactive, parameterized Python scripts that perform the actual data work. Each engine is specialized for a single task (e.g., migrating data, registering a table) and is called by the worker.
-   **Job Queue System (`queue/`, `logs/`, `archive/`)**: A simple but effective file-based queueing system. The UI writes a JSON job file to `queue/`, the worker processes it, writes the output to `logs/`, and moves the completed job file to `archive/`.


## Supported Workflows

The portal provides a unified interface for the following end-to-end data workflows:

1.  **Ingest from Hive to Iceberg**:
    -   **Goal**: Convert traditional Hive tables (e.g., Parquet, ORC) into modern, transaction-safe Apache Iceberg tables within your on-premises data lake.
    -   **Process**: The user selects a source Hive database and one or more tables. The system creates corresponding Iceberg tables in a new namespace (e.g., `source_db_iceberg`).

2.  **Migrate Iceberg to Azure & Register on Snowflake**:
    -   **Goal**: Copy on-prem Iceberg tables to a cloud object store (Azure Blob Storage) and make them queryable in Snowflake.
    -   **Process**: This is a powerful two-stage pipeline triggered by a single user action.
        -   **Stage 1 (Migration)**: The user selects tables from the on-prem Iceberg catalog. The system performs a full data copy to an Iceberg catalog on Azure.
        -   **Stage 2 (Registration)**: If the user opts-in, upon successful migration, the system automatically triggers a second job to register these new Azure-based Iceberg tables as external tables in Snowflake.

3.  **Audit & Reporting**:
    -   **Goal**: Provide complete visibility and traceability for all operations performed through the portal.
    -   **Process**: A dedicated tab in the UI allows users to generate, filter, and view a detailed audit report of all historical jobs. This report can also be exported to Snowflake for advanced analytics.

---

## Setup and Installation

Follow these steps to get the Data Operations Portal running in your environment.

### 1. Prerequisites
-   **Python**: 3.8+
-   **Java**: 8 or 11
-   **Apache Spark**: A local or remote Spark installation accessible from the worker machine.
-   **Required Services**:
    -   Access to an on-premises Hive Metastore.
    -   Azure Blob Storage account credentials.
    -   Snowflake account credentials.

### 2. Clone the Repository
```bash
git clone <your-repository-url>
cd UI/
```

### 3. Install Python Dependencies
It is highly recommended to use a Python virtual environment.
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Your `requirements.txt` file should contain:
```
streamlit
pyspark
pandas
snowflake-connector-python
python-dotenv
```

### 4. Configure Environment Variables (`.env`)
Create a `.env` file in the `UI/` directory. This file will securely store all your credentials. **Never commit the `.env` file to version control.**

```dotenv
# .env

# -- Azure Credentials --
# The access key for your Azure Storage Account
AZURE_KEY="your_azure_storage_account_key_here"


### 5. Configure Engine Scripts
You must review and update the hardcoded configurations within the engine and worker scripts to match your environment. Key files to check:
-   **`worker.py`**: Review the `spark-submit` commands. Ensure all paths, versions, and configurations (e.g., `spark.sql.catalog...`) are correct for your Spark and Hadoop setup.

---

## How to Run the Application

The application requires two separate, long-running processes.

### 1. Start the Backend Worker
In your first terminal, navigate to the `UI/` directory and start the worker. This process will watch the queue for new jobs.
```bash
# Ensure your virtual environment is active
source venv/bin/activate

python3 worker.py
```
Leave this terminal running. You will see log messages here as jobs are processed.

### 2. Start the Streamlit UI
In a second terminal, navigate to the `UI/` directory and start the Streamlit web server.
```bash
# Ensure your virtual environment is active
source venv/bin/activate

streamlit run app.py
```
Your web browser should automatically open to the application's UI. You can now use the portal to run data workflows.

---

## File Breakdown

### Core Application
-   **`app.py`**: The main Streamlit web application script. It defines the UI layout, tabs, buttons, and populates widgets by calling metadata-fetching functions. It submits jobs by creating JSON files in the `queue/` directory.
-   **`worker.py`**: The backend job processor. It runs in a continuous loop, picks up job files from `queue/`, builds the appropriate command, executes the corresponding engine script, and logs the output. It also handles the job chaining logic.
-   **`.env`**: A file to store sensitive credentials like passwords and access keys, keeping them out of the source code.
-   **`config.py`**: (Optional) Can be used to store non-sensitive configurations like default database names or paths.

### Engine Scripts
These scripts perform the actual work and are called by the worker.
-   **`create_multiple_icerberg_tables_hive.py`**: **Engine for Workflow 1.** A `spark-submit` job that reads traditional Hive tables and writes them as new Iceberg tables.
-   **`migrate_via_cli_arg.py`**: **Engine for Workflow 2 (Stage 1).** A `spark-submit` job that reads Iceberg tables from one catalog and writes them to another (e.g., on-prem to Azure).
-   **`register_on_snowflake.py`**: **Engine for Workflow 2 (Stage 2).** A Python script that connects to Spark to find Iceberg metadata and then connects to Snowflake to create or replace external tables.
-   **`export_audit_to_snowflake.py`**: **Engine for Audit Export.** A lightweight Python script that reads the audit CSV and uploads it to a table in Snowflake.

### Utility & Auditing
-   **`generate_audit_report.py`**: A callable Python script containing the logic for the incremental audit report. It reads archived job files, generates a Pandas DataFrame, and saves a summary CSV.
-   **`README.md`**: This file. The central documentation for the project.
-   **`structure.py`**: A utility script, likely used to generate a project directory tree.

### State Directories
-   **`queue/`**: A transient directory where the UI places new job requests as `.json` files.
-   **`logs/`**: Contains the standard output and error logs for every executed job, with filenames corresponding to the job ID.
-   **`archive/`**: All job `.json` files are moved here after they are processed (whether they succeed or fail). This directory serves as the source for the audit report.
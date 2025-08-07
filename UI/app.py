# app.py
import streamlit as st
import subprocess
import uuid
import json
from pathlib import Path
from pyspark.sql import SparkSession
from config import SNOWFLAKE_CONFIG  # Ensure this is defined in your config module

# --- Configuration ---
QUEUE_DIR = Path("queue")
LOGS_DIR = Path("logs")
ARCHIVE_DIR = Path("archive")

# Create directories if they don't exist
QUEUE_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)


# --- Caching for Performance ---
# This prevents re-running the Spark metadata query on every UI interaction.
@st.cache_resource
def get_hive_session():
    """Creates and returns a SparkSession configured for traditional Hive access."""
    print("--- CREATING NEW 'HIVE' SPARK SESSION RESOURCE ---")
    spark = SparkSession.builder \
        .appName("HiveMetadataFetcher") \
        .master("local[*]") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


@st.cache_resource
def get_iceberg_session():
    """Creates and returns a SparkSession configured for Iceberg on Hive Metastore."""
    print("--- CREATING NEW 'ICEBERG' SPARK SESSION RESOURCE ---")
    spark = SparkSession.builder \
        .appName("IcebergMetadataFetcher") \
        .master("local[*]") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive_catalog.type", "hive") \
        .config("spark.sql.catalog.hive_catalog.uri", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

# Use st.cache_data for functions that return serializable data (like a list of strings).
@st.cache_data(ttl=300)
def get_databases(_spark_session, catalog_name=None): # Pass the session as an arg
    """Fetches databases. The result (a list) is cachable data."""
    print(f"--- FETCHING DATABASES (Catalog: {catalog_name}) ---") # Will print only when cache expires
    query = "SHOW DATABASES"
    if catalog_name:
        query += f" IN {catalog_name}"
    
    # Use the passed-in SparkSession
    databases = [row[0] for row in _spark_session.sql(query).collect()]
    return databases

@st.cache_data(ttl=300)
def get_tables(_spark_session, database, catalog_name=None): # Pass the session as an arg
    """Fetches tables. The result (a list) is cachable data."""
    if not database: return []
    print(f"--- FETCHING TABLES (DB: {database}, Catalog: {catalog_name}) ---")
    
    table_path = f"{catalog_name}.{database}" if catalog_name else database
    # Use the passed-in SparkSession
    tables = [row[1] for row in _spark_session.sql(f"SHOW TABLES IN {table_path}").collect()]
    return tables


# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Iceberg Creation and Migration Tool")

tab1, tab2= st.tabs(["Ingest Hive Table to Iceberg", "Migrate Iceberg to Azure"])

hive_spark = get_hive_session()
iceberg_spark = get_iceberg_session() # Get the Spark session resource



with tab1:
    st.header("Workflow 1: Ingest from Traditional Hive")
    st.markdown("Convert existing Hive (Parquet, ORC, etc.) tables into new Apache Iceberg tables in your on-premise `hive_catalog`.")

    hive_dbs = get_databases(hive_spark) # Get default Hive dbs
    selected_hive_db = st.selectbox("Select Source Hive Database", options=hive_dbs, key="hive_db_select")
    
    if selected_hive_db:
        hive_tables = get_tables(hive_spark, selected_hive_db)
        selected_hive_tables = st.multiselect("Select Hive Tables to Ingest", options=hive_tables, key="hive_table_select")
    
    if st.button("Submit Ingestion Job", disabled=(not selected_hive_db or not selected_hive_tables)):
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "job_type": "ingest_from_hive", # <-- This is the new key
            "source_db": selected_hive_db,
            "tables": selected_hive_tables,
            "status": "pending"
        }
        
        job_file_path = QUEUE_DIR / f"{job_id}.json"
        with open(job_file_path, 'w') as f:
            json.dump(job_data, f, indent=4)
            
        st.success(f"Successfully submitted Ingestion Job! Job ID: {job_id}")



with tab2:
    st.header("Workflow 2: Migrate Iceberg Tables to Azure")
    st.markdown("Copy tables from the on-prem `hive_catalog` to the `azure_blob_catalog`.")

    # --- Step 1: Select Source Tables ---
    iceberg_dbs = get_databases(iceberg_spark)
    selected_iceberg_db = st.selectbox("Select Source Iceberg Database from `hive_catalog`", options=iceberg_dbs, key="iceberg_db_select")
    
    selected_iceberg_tables = []
    if selected_iceberg_db:
        iceberg_tables = get_tables(iceberg_spark, selected_iceberg_db)
        selected_iceberg_tables = st.multiselect("Select Iceberg Tables to Migrate", options=iceberg_tables, key="iceberg_table_select")
    
    # --- Step 2: Specify Azure Destination ---
    dest_db_name = st.text_input("Destination Database in `azure_blob_catalog`", value=selected_iceberg_db, key="dest_db_input")
    
    # --- *** NEW: Optional Snowflake Chaining Step *** ---
    st.divider()
    register_sf = st.checkbox("Also register these tables in Snowflake after migration")

    sf_db = ""
    sf_schema = ""
    if register_sf:
        st.subheader("Snowflake Target Details")
        sf_db = st.text_input("Target Snowflake Database", value=SNOWFLAKE_CONFIG.get('database', 'default_db'), key="sf_db_input")
        sf_schema = st.text_input("Target Snowflake Schema", value=SNOWFLAKE_CONFIG.get('schema', 'public'), key="sf_schema_input")

    # --- Step 3: Submit ---
    # Disable button if required fields are missing
    is_disabled = (not selected_iceberg_tables or not dest_db_name)
    if register_sf and (not sf_db or not sf_schema):
        is_disabled = True

    if st.button("Submit Migration Job", disabled=is_disabled):
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "job_type": "migrate_catalogs",
            "source_db": selected_iceberg_db,
            "dest_db": dest_db_name,
            "tables": selected_iceberg_tables,
            "status": "pending",
            # Add the new flag and Snowflake details to the job file
            "register_on_snowflake": register_sf,
            "sf_db": sf_db,
            "sf_schema": sf_schema
        }
        
        with open(QUEUE_DIR / f"{job_id}.json", 'w') as f:
            json.dump(job_data, f, indent=4)
            
        st.success(f"Successfully submitted Migration Job! Job ID: {job_id}")


# --- Job Status Display (remains at the bottom, outside the tabs) ---
st.divider()
st.header("Job Status Dashboard")

all_jobs = []
job_files = list(QUEUE_DIR.glob("*.json")) + list(ARCHIVE_DIR.glob("*.json"))

for job_file in sorted(job_files, key=lambda f: f.stat().st_mtime, reverse=True):
    with open(job_file, 'r') as f:
        all_jobs.append(json.load(f))

if not all_jobs:
    st.info("No jobs submitted yet.")
else:
    for job in all_jobs:
        status = job.get('status', 'unknown')
        job_type_display = job.get('job_type', 'N/A').replace('_', ' ').title()
        
        color = "blue"
        if status == 'completed': color = "green"
        elif status == 'failed': color = "red"
        elif status == 'running': color = "orange"

        with st.expander(f"**{job_type_display}** | Job ID: {job['job_id']} | Status: :{color}[{status.upper()}]"):
            st.json(job)
            log_file = LOGS_DIR / f"{job['job_id']}.log"
            if log_file.exists():
                            # Read log content
                log_content = log_file.read_text()
    
                # Scrollable container for log
                st.markdown(
                    f"""
                    <div style='height: 300px; overflow-y: auto; background-color: #0e1117; padding: 10px; border-radius: 5px;'>
                        <pre style='white-space: pre-wrap; word-wrap: break-word; color: white;'>{log_content}</pre>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                # st.code(log_file.read_text(), language="log")
            else:
                st.warning("Log file not yet available.")
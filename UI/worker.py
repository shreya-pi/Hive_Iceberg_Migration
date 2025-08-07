# worker.py (Updated for Multi-Workflow)
import subprocess
import time
import json
import uuid
from pathlib import Path
from dotenv import load_dotenv
import os

# --- Configuration (no changes here) ---
QUEUE_DIR = Path("queue")
LOGS_DIR = Path("logs")
ARCHIVE_DIR = Path("archive")

load_dotenv()  # Load environment variables if needed

QUEUE_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)

AZURE_KEY =os.getenv("AZURE_KEY")

def update_job_status(job_file_path, status):
    """Updates the status in the job JSON file."""
    if not job_file_path.exists(): return
    with open(job_file_path, 'r+') as f:
        data = json.load(f)
        data['status'] = status
        f.seek(0)
        json.dump(data, f, indent=4)
        f.truncate()

def process_job(job_file_path):
    """Processes a single job file by inspecting its type."""
    with open(job_file_path, 'r') as f:
        job_data = json.load(f)
        
    job_id = job_data["job_id"]
    job_type = job_data["job_type"]
    log_file = LOGS_DIR / f"{job_id}.log"

    print(f"[{time.ctime()}] Found new job: {job_id} (Type: {job_type}). Processing...")
    update_job_status(job_file_path, "running")

    command = []
    # --- COMMAND ROUTING LOGIC ---
    if job_type == "ingest_from_hive":
        print(f"[{time.ctime()}] Building command for Hive to Iceberg ingestion.")


        command = [
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
        "--database",  job_data["source_db"],
        "--tables"
       ]
        command.extend(job_data["tables"])

    elif job_type == "migrate_catalogs":
        print(f"[{time.ctime()}] Building command for cross-catalog migration.")

        command = [
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
            "--conf", f"spark.hadoop.fs.azure.account.key.tulapidemo.blob.core.windows.net={AZURE_KEY}",
            "--conf", "spark.hadoop.fs.azure.header.x-ms-blob-type=BlockBlob",
            "--conf", "spark.hadoop.fs.azure.atomic.rename.dir=/tmp/hdfs-over-wasb-rename-staging",
            "--conf", "spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization=true",
        
            "migrate_via_cli_arg.py",
            "--source-db", job_data["source_db"],
            "--dest-db", job_data["dest_db"],
            "--tables"
        ]
        command.extend(job_data["tables"])

    elif job_type == "register_snowflake":
        print(f"[{time.ctime()}] Building command for Snowflake registration.")
        # Note: This job is run with `python3`, not `spark-submit`
        command = [
            "python3",
            "register_on_snowflake.py",
            "--azure-db", job_data["azure_db"],
            "--sf-db", job_data["sf_db"],
            "--sf-schema", job_data["sf_schema"],
            "--tables"
        ]
        command.extend(job_data["tables"])

    else:
        print(f"[{time.ctime()}] ERROR: Unknown job type '{job_type}' for job {job_id}.")
        update_job_status(job_file_path, "failed")
        # Archive the failed job file
        job_file_path.rename(ARCHIVE_DIR / job_file_path.name)
        return

    # --- EXECUTION LOGIC (no changes here) ---
    job_succeeded = False
    try:
        with open(log_file, 'w') as log:
            subprocess.run(
                command, stdout=log, stderr=subprocess.STDOUT, check=True
            )
        print(f"[{time.ctime()}] Job {job_id} completed successfully.")
        update_job_status(job_file_path, "completed")
        job_succeeded = True
    except subprocess.CalledProcessError:
        print(f"[{time.ctime()}] ERROR: Job {job_id} failed.")
        update_job_status(job_file_path, "failed")
    
    if job_succeeded and job_type == "migrate_catalogs" and job_data.get("register_on_snowflake", False):
        print(f"[{time.ctime()}] Migration successful. Chaining Snowflake registration job.")
        
        # Create a new job file for the Snowflake task
        new_job_id = str(uuid.uuid4())
        snowflake_job_data = {
            "job_id": new_job_id,
            "job_type": "register_snowflake",
            "azure_db": job_data["dest_db"],  # Use the destination DB from the migration
            "sf_db": job_data["sf_db"],       # Get Snowflake info from the original job
            "sf_schema": job_data["sf_schema"],
            "tables": job_data["tables"],     # Use the same list of tables
            "status": "pending",
            "chained_from": job_id # Add a reference to the parent job
        }   

        new_job_file_path = QUEUE_DIR / f"{new_job_id}.json"
        with open(new_job_file_path, 'w') as f:
            json.dump(snowflake_job_data, f, indent=4)
        
        print(f"[{time.ctime()}] Queued new Snowflake job {new_job_id} chained from {job_id}.")


    job_file_path.rename(ARCHIVE_DIR / job_file_path.name)
    print(f"[{time.ctime()}] Archived job file for {job_id}.")

def main():
    print("--- Backend Worker Started (Multi-Workflow Mode) ---")
    print("Watching for jobs in the 'queue' directory...")
    while True:
        pending_jobs = list(QUEUE_DIR.glob("*.json"))
        if pending_jobs:
            process_job(pending_jobs[0])
        else:
            time.sleep(10)

if __name__ == "__main__":
    main()
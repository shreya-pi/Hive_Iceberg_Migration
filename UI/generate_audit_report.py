# generate_audit_report.py (Slightly Modified for Callable Use)
import json
import csv
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import io  # Use io.StringIO to capture console output

# --- Configuration ---
ARCHIVE_DIR = Path("archive")
AUDIT_REPORT_CSV = "job_audit_report.csv"

def calculate_duration(start_iso, end_iso):
    """Calculates duration in seconds between two ISO 8601 timestamp strings."""
    if not start_iso or not end_iso:
        return "N/A"
    try:
        start_time = datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(end_iso.replace('Z', '+00:00'))
        # Ensure they are timezone-aware for correct subtraction
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
            
        duration = (end_time - start_time).total_seconds()
        return f"{duration:.2f}"
    except (ValueError, TypeError):
        return "Error"

def parse_job_file(job_file_path):
    """Parses a single job JSON file and extracts audit information."""
    with open(job_file_path, 'r') as f:
        data = json.load(f)

    job_id = data.get("job_id", "N/A")
    job_type = data.get("job_type", "Unknown").replace('_', ' ').title()
    status = data.get("status", "Unknown").upper()
    start_time = data.get("start_time", None)
    end_time = data.get("end_time", None)
    
    duration_secs = calculate_duration(start_time, end_time)
    
    # Extract details based on job type
    details = ""
    if data.get("job_type") == "ingest_from_hive":
        tables = ", ".join(data.get("tables", []))
        details = f"Source DB: {data.get('source_db')}, Tables: {tables}"
    elif data.get("job_type") == "migrate_catalogs":
        tables = ", ".join(data.get("tables", []))
        details = f"From: {data.get('source_db')} To: {data.get('dest_db')}, Tables: {tables}"
        if data.get("register_on_snowflake"):
            details += f" (Chained SF Reg.)"
    elif data.get("job_type") == "register_snowflake":
        tables = ", ".join(data.get("tables", []))
        details = f"Azure DB: {data.get('azure_db')} -> SF: {data.get('sf_db')}.{data.get('sf_schema')}, Tables: {tables}"
        if data.get("chained_from"):
            details += f" (Chained from {data.get('chained_from')[:8]}...)"
            
    return {
        "Job ID": job_id,
        "Job Type": job_type,
        "Status": status,
        "Start Time (UTC)": start_time,
        "End Time (UTC)": end_time,
        "Duration (s)": duration_secs,
        "Details": details
    }


def generate_report(force_full_rescan=False):
    """
    Generates the audit report. Always returns the full, most up-to-date DataFrame.
    """
    print(f"--- Generating Audit Report (Incremental: {not force_full_rescan}) ---")

    existing_jobs_df = pd.DataFrame()
    processed_job_ids = set()

    # --- Step 1: Read existing report ---
    if Path(AUDIT_REPORT_CSV).exists() and not force_full_rescan:
        try:
            existing_jobs_df = pd.read_csv(AUDIT_REPORT_CSV, dtype={'Job ID': str})
            if 'Job ID' in existing_jobs_df.columns:
                processed_job_ids = set(existing_jobs_df['Job ID'])
            print(f"Loaded {len(existing_jobs_df)} previously audited jobs.")
        except Exception as e:
            print(f"Warning: Could not read existing audit report. Performing a full rescan. Error: {e}")
            existing_jobs_df = pd.DataFrame(); processed_job_ids = set()

    # --- Step 2: Find and parse new job files ---
    if not ARCHIVE_DIR.exists():
        return None, "Archive directory does not exist."
    
    new_files_to_process = [
        f for f in ARCHIVE_DIR.glob("*.json") if f.stem not in processed_job_ids
    ]
    
    newly_parsed_jobs = []
    if new_files_to_process:
        print(f"Found {len(new_files_to_process)} new job files to parse.")
        for job_file in new_files_to_process:
            try:
                newly_parsed_jobs.append(parse_job_file(job_file))
            except Exception as e:
                print(f"Warning: Could not parse new file {job_file.name}. Error: {e}")

    # --- Step 3: Combine old and new data ---
    new_jobs_df = pd.DataFrame(newly_parsed_jobs)
    all_job_data_df = pd.concat([existing_jobs_df, new_jobs_df], ignore_index=True)

    if all_job_data_df.empty:
        return None, "No completed jobs found to report."

    # --- Step 4: Sort, Save, and Prepare Return Values ---
    status_message = ""
    try:
        # Sort by End Time descending. Convert to datetime for proper sorting.
        # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
        if 'End Time (UTC)' in all_job_data_df.columns:
            all_job_data_df['End Time (UTC)'] = pd.to_datetime(all_job_data_df['End Time (UTC)'], errors='coerce')
            # Sort, putting any NaT values (e.g., for running jobs) at the end
            all_job_data_df = all_job_data_df.sort_values(by='End Time (UTC)', ascending=False, na_position='last')

        # Overwrite the CSV with the full, sorted list
        all_job_data_df.to_csv(AUDIT_REPORT_CSV, index=False, encoding='utf-8')
        
        if not newly_parsed_jobs:
            status_message = "Audit report is already up-to-date."
        else:
            status_message = f"Report updated with {len(newly_parsed_jobs)} new job(s)."
        
        print(f"--- {status_message} ---")
            
    except Exception as e:
        error_message = f"ERROR: Could not process or write report. Error: {e}"
        print(f"--- {error_message} ---")
        return all_job_data_df, error_message

    # ALWAYS return the full, sorted DataFrame.
    return all_job_data_df, status_message


# This block allows the script to be run directly from the command line
if __name__ == "__main__":
    # report_string, _ = generate_report()
    # print(report_string)
    import argparse
    parser = argparse.ArgumentParser(description="Generate an incremental audit report of completed jobs.")
    parser.add_argument(
        "--full-rescan",
        action="store_true",
        help="Ignore the existing report and re-read all JSON files from the archive."
    )
    args = parser.parse_args()

    report_string, _ = generate_report(force_full_rescan=args.full_rescan)
    print(report_string)



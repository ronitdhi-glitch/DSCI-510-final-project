import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from kaggle_service import download_kaggle_dataset
from job_filter import filter_high_risk_jobs
from db_setup import create_connection, create_table
from csv_to_db import insert_data_from_csv

import sqlite3


def fetch_supplemental_jobs(db_path="../database/jobs_data.db"):
    """Fetch all supplemental task jobs (most likely to be automated)."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    SELECT id, onet_soc_code, title, task_id, task, task_type
    FROM task_statements
    WHERE LOWER(task_type) = 'supplemental'
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results


def main():
    print("\n===== AI JOB LOSS ANALYSIS STARTED =====\n")

    print("Downloading Kaggle dataset...")

    csv_path = download_kaggle_dataset()

    # Output CSV path (goes to data folder OUTSIDE src)
    filtered_output = os.path.join("../data", "jobs_likely_to_lose.csv")

    high_risk_jobs = filter_high_risk_jobs(csv_path, filtered_output)

    print(f"High-risk job records: {len(high_risk_jobs)}")
    print(f"Saved filtered CSV to: {filtered_output}\n")


    print("Setting up database...")

    conn = create_connection()
    create_table(conn)
    conn.close()

    print("Inserting CSV data into database...")
    insert_data_from_csv()


    print("\nFetching supplemental jobs...")
    supplemental_jobs = fetch_supplemental_jobs()

    print(f"\nTotal supplemental tasks: {len(supplemental_jobs)}")
    print("\n=== SAMPLE JOBS LIKELY TO LOSE WORK (FIRST 10) ===")

    for job in supplemental_jobs[:10]:
        print(f"ID: {job[0]} | SOC: {job[1]} | Title: {job[2]} | Task: {job[4]}")

    if len(supplemental_jobs) > 10:
        print(f"\n...and {len(supplemental_jobs) - 10} more jobs.\n")

    print("\n===== ANALYSIS COMPLETE =====\n")


if __name__ == "__main__":
    main()

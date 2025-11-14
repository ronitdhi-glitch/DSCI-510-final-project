import pandas as pd
import sqlite3
import os

def insert_data_from_csv(
    csv_path=None,
    db_path=None
):
    """Read all data from CSV and insert into database."""

    # Base directory of this file (src/)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Default CSV path: ../data/Task Statements.csv
    if csv_path is None:
        csv_path = os.path.join(base_dir, "..", "data", "Task Statements.csv")

    # Default DB path: ../database/jobs_data.db
    if db_path is None:
        db_path = os.path.join(base_dir, "..", "database", "jobs_data.db")

    # Ensure database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Debug print (optional)
    print(f"Using CSV path: {csv_path}")
    print(f"Using DB path: {db_path}")

    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"CSV loaded successfully! Total rows: {len(df)}")

    # Clean and rename columns
    df.columns = [col.strip() for col in df.columns]

    df.rename(columns={
        'O*NET-SOC Code': 'onet_soc_code',
        'Title': 'title',
        'Task ID': 'task_id',
        'Task': 'task',
        'Task Type': 'task_type',
        'Incumbents Responding': 'incumbents_responding',
        'Date': 'date',
        'Domain Source': 'domain_source'
    }, inplace=True)

    # Insert into DB
    conn = sqlite3.connect(db_path)
    df.to_sql('task_statements', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

    print(f"All {len(df)} rows inserted successfully into database!")
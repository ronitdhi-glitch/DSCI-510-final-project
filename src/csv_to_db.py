import pandas as pd
import sqlite3

def insert_data_from_csv(csv_path="../data/Task Statements.csv", db_path="../database/jobs_data.db"):
    """Read all data from CSV and insert into database."""

    df = pd.read_csv(csv_path)
    print(f" CSV loaded successfully! Total rows: {len(df)}")

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


    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    df.to_sql('task_statements', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print(f"All {len(df)} rows inserted successfully into database!")

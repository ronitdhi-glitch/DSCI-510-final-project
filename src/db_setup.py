import sqlite3
import os

def create_connection(db_path="../database/jobs_data.db"):
    """Create and return a database connection."""

    # Ensure the folder exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    return conn


def create_table(conn):
    """Create the task_statements table."""
    query = """
    CREATE TABLE IF NOT EXISTS task_statements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        onet_soc_code TEXT,
        title TEXT,
        task_id TEXT,
        task TEXT,
        task_type TEXT,
        incumbents_responding INTEGER,
        date TEXT,
        domain_source TEXT
    )
    """
    conn.execute(query)
    conn.commit()
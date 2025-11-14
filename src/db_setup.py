import sqlite3

def create_connection(db_path="../database/jobs_data.db"):
    """Create and return a database connection."""
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

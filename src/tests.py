import os
import pandas as pd
import sqlite3
import pytest


from src.kaggle_service import download_kaggle_dataset
from src.job_filter import filter_high_risk_jobs
from src.db_setup import create_connection, create_table
from src.csv_to_db import insert_data_from_csv
from src.main import fetch_supplemental_jobs




def test_kaggle_download_file_exists(monkeypatch):
    """
    This test mocks Kaggle API and checks if the function
    returns correct file path.
    """

    def mock_download():
       
        os.makedirs("data", exist_ok=True)
        path = "data/mock.csv"
        with open(path, "w") as f:
            f.write("Risk of Automation\nHigh\nLow")
        return path

    monkeypatch.setattr("src.kaggle_service.download_kaggle_dataset", mock_download)

    csv_path = download_kaggle_dataset()
    assert os.path.exists(csv_path)




def test_filter_high_risk_jobs():
    df = pd.DataFrame({
        "Job": ["A", "B", "C"],
        "Risk of Automation": ["High", "Low", "High"]
    })

    test_csv = "data/test_input.csv"
    out_csv = "data/test_output.csv"

    df.to_csv(test_csv, index=False)

    filtered = filter_high_risk_jobs(test_csv, out_csv)

    assert len(filtered) == 2
    assert os.path.exists(out_csv)




def test_database_creation():
    conn = create_connection("tests/test.db")
    create_table(conn)

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='task_statements';")
    result = cursor.fetchone()

    assert result is not None  

    conn.close()




def test_insert_csv_to_db():
    conn = create_connection("tests/test2.db")
    create_table(conn)
    conn.close()

    df = pd.DataFrame({
        "id": [1],
        "onet_soc_code": ["00-0000"],
        "title": ["Tester"],
        "task_id": ["T-001"],
        "task": ["Testing job"],
        "task_type": ["supplemental"]
    })

    os.makedirs("data", exist_ok=True)
    df.to_csv("data/test_csv.csv", index=False)

    insert_data_from_csv("data/test_csv.csv", "tests/test2.db")

    conn = sqlite3.connect("tests/test2.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM task_statements")
    rows = cursor.fetchall()

    assert len(rows) == 1  

    conn.close()




def test_fetch_supplemental_jobs():
    conn = sqlite3.connect("tests/test3.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE task_statements (
            id INTEGER PRIMARY KEY,
            onet_soc_code TEXT,
            title TEXT,
            task_id TEXT,
            task TEXT,
            task_type TEXT
        )
    """)

    cursor.execute("""
        INSERT INTO task_statements (id, onet_soc_code, title, task_id, task, task_type)
        VALUES (1, '11-1111', 'Test Role', 'T-001', 'Sample Task', 'supplemental')
    """)

    conn.commit()
    conn.close()

    results = fetch_supplemental_jobs("tests/test3.db")

    assert len(results) == 1
    assert results[0][2] == "Test Role"
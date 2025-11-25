import pandas as pd
import os

def load_state_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "..", "data", "automation_data_by_state.csv")
    path = os.path.abspath(path)

    print("Loading CSV from:", path)

    # Try UTF-8 first, then fallback
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        print("⚠ UTF-8 failed, retrying with latin1 encoding...")
        df = pd.read_csv(path, encoding="latin1")

    # Normalize columns
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    return df
import pandas as pd
from config import AUTOMATION_STATE_FILE

def load_state_data():
    print("Loading CSV from:", AUTOMATION_STATE_FILE)

    try:
        df = pd.read_csv(AUTOMATION_STATE_FILE, encoding="utf-8")
    except UnicodeDecodeError:
        print("⚠ UTF-8 failed, retrying with latin1 encoding...")
        df = pd.read_csv(AUTOMATION_STATE_FILE, encoding="latin1")

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

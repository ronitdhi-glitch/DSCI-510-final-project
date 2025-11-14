
import pandas as pd
import os

# Dynamically locate CSV file in ../data/
BASE_DIR = os.path.dirname(__file__)                # .../src
DATA_PATH = os.path.join(BASE_DIR, "..", "data", "ai_job_trends_dataset.csv")

def load_data():
    """Load the dataset from the data folder."""
    try:
        df = pd.read_csv(DATA_PATH)
        print(f"[INFO] Loaded dataset successfully from {DATA_PATH}")
        return df
    except FileNotFoundError:
        print("[ERROR] CSV file not found. Check your data folder path.")
        return pd.DataFrame()


def filter_high_risk_jobs(df, risk_threshold=80):
    """Filter jobs with high automation risk or AI impact."""
    if df.empty:
        return pd.DataFrame()
    
    risky = df[
        (df["Automation Risk (%)"] > risk_threshold) |
        (df["AI Impact Level"].str.lower() == "high")
    ]
    return risky


def summarize_industry_risk(df):
    """Summarize high-risk jobs by industry."""
    if df.empty:
        return {}
    return df.groupby("Industry")["Job Title"].count().to_dict()


def top_high_risk_jobs(df, n=10):
    """Return top N jobs most likely to be lost to AI."""
    if df.empty:
        return pd.DataFrame()
    return (
        df[["Job Title", "Industry", "AI Impact Level", "Automation Risk (%)"]]
        .sort_values(by="Automation Risk (%)", ascending=False)
        .head(n)
    )


if __name__ == "__main__":
    # Test the module
    data = load_data()
    risky_jobs = filter_high_risk_jobs(data)
    print(top_high_risk_jobs(risky_jobs))

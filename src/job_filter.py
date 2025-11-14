import pandas as pd
import os

def filter_high_risk_jobs(csv_path, output_path):
    """
    Reads the dataset and filters jobs where AI Impact Level = 'High'.
    """

    df = pd.read_csv(csv_path)

    # Normalize values to avoid issues with capitalization or spaces
    df["AI Impact Level"] = df["AI Impact Level"].astype(str).str.strip().str.lower()

    # Filter only high AI impact jobs
    high_risk = df[df["AI Impact Level"] == "high"]

    # Make sure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    high_risk.to_csv(output_path, index=False)

    return high_risk

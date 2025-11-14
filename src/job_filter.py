import pandas as pd
import os

def filter_high_risk_jobs(csv_path, output_path):
    """
    Reads the dataset and filters jobs where Risk of Automation = 'High'
    """

    df = pd.read_csv(csv_path)

    # Only keep risky jobs
    high_risk = df[df["Risk of Automation"] == "High"]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    high_risk.to_csv(output_path, index=False)

    return high_risk

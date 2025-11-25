import pandas as pd

def calculate_state_risky_jobs(file_path):
    df = pd.read_csv(file_path, encoding="latin1")  

    california_jobs = df["California"]
    probabilities = df["Probability"]

    median_prob = probabilities.median()
    high_risk = df[probabilities >= median_prob]

    total_risky_jobs = (high_risk["California"] * high_risk["Probability"]).sum()

    risky_csv_path = "data/risky_jobs_california.csv"
    high_risk.to_csv(risky_csv_path, index=False)

    return total_risky_jobs, median_prob, risky_csv_path

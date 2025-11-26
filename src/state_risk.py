import pandas as pd
# This function calculates nimber of high risk automation jobs specifically for the state of california. 
def calculate_state_risky_jobs(file_path):
    df = pd.read_csv(file_path, encoding="latin1")  

    california_jobs = df["California"]
    probabilities = df["Probability"]

    median_prob = probabilities.median()
    high_risk = df[probabilities >= median_prob]
    # multiply California employment by automation probability to estimate toal number of risky jobs in the state. 
    total_risky_jobs = (high_risk["California"] * high_risk["Probability"]).sum()

    risky_csv_path = "data/risky_jobs_california.csv"
    high_risk.to_csv(risky_csv_path, index=False)

    return total_risky_jobs, median_prob, risky_csv_path


import pandas as pd

def get_supplemental_jobs(csv_path):
    """
    Reads the CSV file and returns only the rows 
    where Task Type = 'supplemental'.
    """
    df = pd.read_csv(csv_path)

    supplemental_df = df[df["Task Type"].str.lower() == "supplemental"]

    return supplemental_df
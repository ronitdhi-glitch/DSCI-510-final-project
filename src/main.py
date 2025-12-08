import os
import sys
import pandas as pd

# Import constants from config
from config import (
    BASE_DIR,
    AI_JOB_TRENDS_FILE,
    TASK_STATEMENTS_FILE,
    AUTOMATION_STATE_FILE,
    COUNTY_POP_FILE,
    SUPPLEMENTAL_OUTPUT_FILE,
    DRIVE_LINKS,
    DOWNLOADED_FILE_NAMES
)

sys.path.append(BASE_DIR)

# Local modules imports for the project
from kaggle_service import download_kaggle_dataset
from job_filter import filter_high_risk_jobs as filter_high_risk_jobs_kaggle
from data_filter import get_supplemental_jobs
from load_data import load_state_data
from state_analysis import (
    filter_high_risk_jobs as filter_high_risk_jobs_state,
    get_top_states,
    get_bottom_states,
)
from visualization import plot_top_10, plot_bottom_10, plot_distribution
from state_risk import calculate_state_risky_jobs
from county_risk import calculate_county_cost
from utils import plot_top_risk_jobs
from charts import generate_all_graphs
from downloader import download_file

from file_manager import create_data_folder



def run_kaggle_ai_job_loss_analysis():
    print("\n****** AI Jobs Analysis Beginning ****** \n")
    print("Downloading Kaggle dataset...")

    csv_path = download_kaggle_dataset()
    high_risk_jobs = filter_high_risk_jobs_kaggle(csv_path, AI_JOB_TRENDS_FILE)

    print(f"High-risk job records: {len(high_risk_jobs)}")
    print(f"Saved filtered CSV to: {AI_JOB_TRENDS_FILE}\n")
    print("\n****** AI Job Analysis Finished ******\n")


def run_supplemental_filter():
    result = get_supplemental_jobs(TASK_STATEMENTS_FILE)

    print("\nJobs with supplemental task type:\n")
    print(result)

    result.to_csv(SUPPLEMENTAL_OUTPUT_FILE, index=False)
    print(f"\nFiltered data saved to: {SUPPLEMENTAL_OUTPUT_FILE}")


def run_state_risk_analysis():
    print("\n STATE HIGH-RISK ANALYSIS")

    df = load_state_data()
    risky_jobs = filter_high_risk_jobs_state(df)

    print("\nTotal high-risk occupations:", len(risky_jobs))
    top10 = get_top_states(risky_jobs)
    bottom10 = get_bottom_states(risky_jobs)

    print("\nTop 10 states at highest risk:")
    print(top10)
    print("\nBottom 10 states at lowest risk:")
    print(bottom10)

    plot_top_10(top10)
    plot_bottom_10(bottom10)
    plot_distribution(df)

    print("\nAll graphs saved inside /reports folder.")


def run_california_automation_analysis():
    print("\n CALIFORNIA AUTOMATION RISK")
    print(" Starting automation risk analysis for California...\n")

    total_risky_jobs, median_prob, risky_csv = calculate_state_risky_jobs(AUTOMATION_STATE_FILE)

    print(" STATE-LEVEL RESULTS")
    print(f"   • Total high-risk jobs: {total_risky_jobs:.2f}")
    print(f"   • Median probability of automation: {median_prob:.3f}")

    print("\n Preview: risky_jobs_california.csv")
    risky_df = pd.read_csv(risky_csv)
    print(risky_df.head(10))

    county_output = calculate_county_cost(COUNTY_POP_FILE, total_risky_jobs)

    print("\nCOUNTY-LEVEL RESULTS")
    county_df = pd.read_csv(county_output)
    print(county_df.head(10))

    plot_top_risk_jobs(risky_csv)
    generate_all_graphs(county_output, risky_csv)
    print("\n All Analysis Completed")



def main():
    print("Step 1: Ensuring data folder exists...\n")

   
    data_folder = create_data_folder()
    print(f"Using data folder: {data_folder}\n")



    print("Step 2: Downloading required CSV datasets...\n")

    # Download files into the created data folder
    for link, name in zip(DRIVE_LINKS, DOWNLOADED_FILE_NAMES):
        download_file(link, name, data_folder)

    print("\n Files downloaded successfully. Proceeding to analysis...\n")

    run_kaggle_ai_job_loss_analysis()
    run_supplemental_filter()
    run_state_risk_analysis()
    run_california_automation_analysis()



# Required for standalone execution
if __name__ == "__main__":
    main()


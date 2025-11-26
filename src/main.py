import os
import sys
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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

def run_kaggle_ai_job_loss_analysis():
    print("\n****** AI Jobs Analysis Beginning ****** \n")

    print("Downloading Kaggle dataset...")
    csv_path = download_kaggle_dataset()

    
    filtered_output = os.path.join(BASE_DIR, "..", "data", "ai_job_trends_dataset.csv")

    high_risk_jobs = filter_high_risk_jobs_kaggle(csv_path, filtered_output)

    print(f"High-risk job records: {len(high_risk_jobs)}")
    print(f"Saved filtered CSV to: {filtered_output}\n")

    print("\n****** AI Job Analysis Finished ******\n")
    
def run_supplemental_filter():
    # Filter occupations that include "Supplemental" task type. 
    csv_file = os.path.join(BASE_DIR, "..", "data", "Task Statements.csv")
    result = get_supplemental_jobs(csv_file)

    print("\nJobs with supplemental task type:\n")
    print(result)

    # Save output
    output_path = os.path.join(BASE_DIR, "..", "data", "supplemental_jobs.csv")
    result.to_csv(output_path, index=False)

    print(f"\nFiltered data saved to: {output_path}")
    
# State High-Risk Job Analysis

def run_state_risk_analysis():
    # Analyze high risk occupations at US state level. 
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

    
    automation_data = os.path.join(BASE_DIR, "..", "data", "automation_data_by_state.csv")
    county_data = os.path.join(BASE_DIR, "..", "data", "co-est2024-pop-06.csv")
    total_risky_jobs, median_prob, risky_csv = calculate_state_risky_jobs(automation_data)

    print(" STATE-LEVEL RESULTS")
    print(f"   • Total high-risk jobs: {total_risky_jobs:.2f}")
    print(f"   • Median probability of automation: {median_prob:.3f}")

    
    print("\n Preview: risky_jobs_california.csv")
    risky_df = pd.read_csv(risky_csv)
    print(risky_df.head(10))
    

    
    county_output = calculate_county_cost(county_data, total_risky_jobs)

    print("\nCOUNTY-LEVEL RESULTS")
    county_df = pd.read_csv(county_output)
    print(county_df.head(10))
    

    
    plot_top_risk_jobs(risky_csv)
    generate_all_graphs(county_output, risky_csv)
    print("/n All Analysis Compelted")
    
def main():
    
    run_kaggle_ai_job_loss_analysis()
    run_supplemental_filter()
    run_state_risk_analysis()
    run_california_automation_analysis()


if __name__ == "__main__":
    main()


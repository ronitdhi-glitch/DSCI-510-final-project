import os

def save_summary(top, bottom, total_jobs):
    # Get base directory of this script
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Build path to /reports
    reports_dir = os.path.join(base_dir, "..", "reports")
    os.makedirs(reports_dir, exist_ok=True)  # Ensure folder exists

    # Final summary file path
    summary_path = os.path.join(reports_dir, "summary.txt")

    # Write summary
    with open(summary_path, "w") as f:
        f.write("AI JOB LOSS ANALYSIS REPORT\n")
        
        
        f.write(f"Total Jobs Predicted to be Lost: {total_jobs}\n\n")
        
        f.write("Top 10 States:\n")
        f.write(top.to_string())
        f.write("\n\nBottom 10 States:\n")
        f.write(bottom.to_string())

    print(f"Summary saved at: {summary_path}")

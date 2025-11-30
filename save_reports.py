import os
from config import REPORTS_DIR

def save_summary(top, bottom, total_jobs):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    summary_path = os.path.join(REPORTS_DIR, "summary.txt")

    with open(summary_path, "w") as f:
        f.write("AI JOB LOSS ANALYSIS REPORT\n")
        f.write(f"Total Jobs Predicted to be Lost: {total_jobs}\n\n")
        f.write("Top 10 States:\n")
        f.write(top.to_string())
        f.write("\n\nBottom 10 States:\n")
        f.write(bottom.to_string())

    print(f"Summary saved at: {summary_path}")

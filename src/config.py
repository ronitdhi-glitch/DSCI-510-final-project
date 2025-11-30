import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

AI_JOB_TRENDS_FILE = os.path.join(BASE_DIR, "..", "data", "ai_job_trends_dataset.csv")
TASK_STATEMENTS_FILE = os.path.join(BASE_DIR, "..", "data", "Task Statements.csv")
AUTOMATION_STATE_FILE = os.path.join(BASE_DIR, "..", "data", "automation_data_by_state.csv")
COUNTY_POP_FILE = os.path.join(BASE_DIR, "..", "data", "co-est2024-pop-06.csv")
SUPPLEMENTAL_OUTPUT_FILE = os.path.join(BASE_DIR, "..", "data", "supplemental_jobs.csv")


DRIVE_LINKS = [
    "https://drive.google.com/file/d/1_jtWgv3Hcb9e4ACOwuBpoPJcMXlz7hiQ/view?usp=sharing",
    "https://drive.google.com/file/d/1LOCUEaWkbuDs1qLqipGCbhUp4u63SQAX/view?usp=sharing",
    "https://drive.google.com/file/d/1CaNfVkGenjCRr8MKgiSQO5sSyUXY1kVU/view?usp=sharing"
]

# CSV file names for downloads
DOWNLOADED_FILE_NAMES = [
    "Task Statements.csv",
    "co-est2024-pop-06.csv",
    "automation_data_by_state.csv"
]


DATASET = "sahilislam007/ai-impact-on-job-market-20242030"
DOWNLOAD_DIR = "data"
CSV_NAME = "ai_job_trends_dataset.csv"      # Extracted Kaggle CSV name


REPORTS_DIR = os.path.join(BASE_DIR, "..", "reports")

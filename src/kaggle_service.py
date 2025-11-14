import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi


DATASET = "sahilislam007/ai-impact-on-job-market-20242030"
DOWNLOAD_DIR = "data"
CSV_NAME = "Ai_Impact_On_Job_Market.csv"  # Kaggle file inside ZIP


def download_kaggle_dataset():
    """
    Downloads the Kaggle dataset if not already downloaded.
    Returns: path to extracted CSV file.
    """

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    zip_path = os.path.join(DOWNLOAD_DIR, "dataset.zip")

    if not os.path.exists(zip_path):
        print("Downloading Kaggle dataset...")
        api.dataset_download_files(DATASET, path=DOWNLOAD_DIR, unzip=False)
    else:
        print("Dataset already downloaded. Skipping download.")

    print("Extracting ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DOWNLOAD_DIR)

    csv_path = os.path.join(DOWNLOAD_DIR, CSV_NAME)
    return csv_path

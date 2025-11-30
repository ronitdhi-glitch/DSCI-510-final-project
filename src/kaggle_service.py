import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

from config import DATASET, DOWNLOAD_DIR, CSV_NAME


def download_kaggle_dataset():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    zip_path = os.path.join(DOWNLOAD_DIR, "ai-impact-on-job-market-20242030.zip")

    if not os.path.exists(zip_path):
        print("Downloading Kaggle dataset...")
        api.dataset_download_files(DATASET, path=DOWNLOAD_DIR, unzip=False)
    else:
        print("Dataset already downloaded. Skipping.")

    print("Extracting ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DOWNLOAD_DIR)

    return os.path.join(DOWNLOAD_DIR, CSV_NAME)

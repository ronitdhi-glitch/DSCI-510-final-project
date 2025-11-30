import os
import requests

# Convert Google Drive link to direct download link
def get_direct_download_url(drive_url: str) -> str:
    file_id = drive_url.split('/d/')[1].split('/')[0]
    return f"https://drive.google.com/uc?export=download&id={file_id}"

# Download CSV file and save to data folder
def download_file(drive_url: str, filename: str, folder_path: str):
    download_url = get_direct_download_url(drive_url)
    save_path = os.path.join(folder_path, filename)

    print(f"Downloading {filename} ...")
    response = requests.get(download_url, allow_redirects=True)

    if response.status_code == 200:
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"Saved: {save_path}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
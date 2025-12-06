import os

def create_data_folder():
    # Current file (inside src folder)
    src_dir = os.path.dirname(os.path.abspath(__file__))

    # Move one level up → project root
    project_root = os.path.dirname(src_dir)

    # Path to data folder in project root
    data_folder = os.path.join(project_root, "data")

    # Create folder if it does not exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Folder created: {data_folder}")
    else:
        print("Data folder already exists.")

    return data_folder   

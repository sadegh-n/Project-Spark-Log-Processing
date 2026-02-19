import os
from kaggle.api.kaggle_api_extended import KaggleApi


def download_data(dataset_id: str, output_dir: str):
    """
    Downloads and unzips a Kaggle dataset to the specified directory.
    """
    api = KaggleApi()
    api.autenticate()

    os.makedirs(output_dir, exist_ok = True)

    api.dataset_download_files(dataset_id, path = output_dir, unzip = True)

if __name__ == '__main__':
    dataset_id = 'eliasdabbas/web-server-access-logs'
    data_path = 'data/raw'

    download_data(dataset_id, data_path)


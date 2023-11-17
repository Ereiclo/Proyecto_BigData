import requests
import os
from tqdm import tqdm  
from definitions.path import ROOT_DIR


def fetch_data():
    if not os.path.exists(ROOT_DIR + "/data"):
        os.makedirs(ROOT_DIR + "/data")

    url = 'https://drive.google.com/uc?id=1ZRCBMZGEGIL4wroLtLecGeA255rknJol&confirm=t'
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    file = open(ROOT_DIR + "/data/supermercados.csv", "wb")

    progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)

    for chunk in response.iter_content(chunk_size=8192):
        progress_bar.update(len(chunk))
        file.write(chunk)

    progress_bar.close()
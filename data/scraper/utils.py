import re
import os
import zipfile
import requests
from datetime import datetime

from bs4 import BeautifulSoup
from tqdm import tqdm

urls = [
    "http://data.gdeltproject.org/gkg/index.html",
    "http://data.gdeltproject.org/events/index.html"
]
download_urls = [
    "http://data.gdeltproject.org/gkg/",
    "http://data.gdeltproject.org/events/"
]   

pattern = re.compile(r'(\d{8})\.(gkg(?:counts)?|export)\.(csv|CSV)\.zip')

# Function to check if a date is within the specified range
def is_within_date_range(
        date_str:str,
        start_date:datetime,
        end_date:datetime
    )->bool:
    date = datetime.strptime(date_str, "%Y%m%d")
    return (start_date <= date) and (date <= end_date)

def get_urls(
        start_date: datetime = datetime(2024,8,10),
        end_date: datetime = datetime(2024,8,13),
    )-> list[str]:
    # Define a regular expression to match the required formats
    # YYYYMMDD.gkg.csv.zip or YYYYMMDD.gkgcounts.csv.zip

    filtered_urls = []
    for i in range(2):
        url = urls[i]
        download_url = download_urls[i]

        # Send a GET request to the GDELT GKG website
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup 
            # (https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all links on the page
            print("Discovering links...")
            links = soup.find_all('a')

            # Filter links that match the specified formats and date range
            inside_range = False
            # Number of files to be fetched (2 per day, including start and end date)
            total_files = ((end_date - start_date).days + 1)*2
            bar = tqdm(desc="Filtering urls", total=total_files, ncols=100)
            for link in links:
                href = link.get('href')
                if href:
                    match = pattern.search(href)
                    if match:
                        date_str = match.string.split(".")[0]
                        if is_within_date_range(date_str, start_date, end_date):
                            filtered_urls.append(f"{download_url}{href}")
                            inside_range = True
                            bar.update(1)
                        else:
                            # Break for loop once it lefts the specified date range
                            if inside_range: break
                    
        else:
            print(f"Failed to get index. Status code: {response.status_code}")
        
    return filtered_urls

def download_file(url: str, file_path: str, file_name: str):
    # Send GET request to download the file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        # Write the content to the local file
        with open(file_path, 'wb') as f, tqdm(
            desc=f"Downloading file {file_name}",
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            ncols=100,
        ) as bar:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
                bar.update(len(chunk))

def unzip_file(file_path, folder_name):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(folder_name)
    os.remove(file_path)

def download_files(urls: list[str]=[]):
    bar = tqdm(desc="Downloanding files", total=len(urls), ncols=100)
    for url in urls:
        file_name = url.split("/")[-1]
        date_str = file_name.split(".")[0]
        folder_name = f"./files/{date_str}"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        file_path = os.path.join(folder_name, file_name)
        download_file(url, file_path, file_name)
        unzip_file(file_path, folder_name)
        bar.update(1)
    return
import argparse
from datetime import datetime

from utils import download_files, get_urls, add_col_to_event_data
import os

def list_files_recursive(directory):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Short sample app')
    parser.add_argument('--sd', action="store", dest='start_date', default="20240810")
    parser.add_argument('--ed', action="store", dest='end_date', default="20240813")
    args = parser.parse_args()
    start_date = datetime.strptime(args.start_date, "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")
    urls = get_urls(start_date, end_date)
    download_files(urls)

    file_list = list_files_recursive('./files')
    for f in file_list:
        if f.endswith('.export.CSV'):
            add_col_to_event_data(f)
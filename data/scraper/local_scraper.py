import argparse
from datetime import datetime

from utils import download_files, get_urls

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Short sample app')
    parser.add_argument('--sd', action="store", dest='start_date', default="20240810")
    parser.add_argument('--ed', action="store", dest='end_date', default="20240813")
    args = parser.parse_args()
    start_date = datetime.strptime(args.start_date, "%Y%m%d")
    end_date = datetime.strptime(args.end_date, "%Y%m%d")
    urls = get_urls(start_date, end_date)
    download_files(urls)
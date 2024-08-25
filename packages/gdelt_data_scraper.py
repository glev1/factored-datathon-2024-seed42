from concurrent.futures import ThreadPoolExecutor
from packages.utils import download_files, get_urls, add_col_to_event_data
import os
from datetime import datetime

class GDELTDataScraper:
    def __init__(self):
        pass

    def list_files_recursive(self, directory):
        file_list = []
        for root, _, files in os.walk(directory):
            for file in files:
                file_list.append(os.path.join(root, file))
        return file_list

    def process_file(self, file_path):
        if file_path.endswith('.export.CSV'):
            add_col_to_event_data(file_path)

    def scrape_data(self, start_date, end_date):
        urls = get_urls(start_date, end_date)
        download_files(urls)

        file_list = self.list_files_recursive('./files')
        with ThreadPoolExecutor() as executor:
            executor.map(self.process_file, file_list)
import os
import time
import random
import requests

"""
Python Script to retrieve index files at SEC-EDGAR
"""

path_default = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) + '/Data/raw/index'

base_url = "https://www.sec.gov/Archives/edgar/full-index/"
headers = {'User-Agent': 'Individual Andres Oliva andresolivadenis@gmail.com',
           'Accept-Encoding': 'gzip, deflate',
           'Host': 'www.sec.gov'}

# year_list = list(range(2021, 2022, 1))
year_list = list(range(2021, 2023, 1))
quarter_lists = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
archive_file_list = ["xbrl.idx", "master.idx"]  # ["company.idx", "xbrl.idx", "master.idx", "crawler.idx"]

sleep_interval = [0.5, 2]

for year in year_list:
    year_str = str(year)
    for quarter in quarter_lists:
        for archive_file in archive_file_list:
            url = base_url + "/{}/{}/{}".format(year_str, quarter, archive_file)

            print("\nRetrieving url: ", url)

            response = requests.get(url, headers=headers)
            response_text = response.text

            print("\t Retrieve Done, saving file ...")

            directory = os.path.join(path_default, os.path.join(year_str, quarter))
            if not os.path.exists(directory):
                os.makedirs(directory)

            path_file = os.path.join(directory, archive_file)

            with open(path_file, 'w') as f:
                f.write(response.text)

            sleep_time = random.random()*(sleep_interval[1] - sleep_interval[0]) + sleep_interval[0]
            print("\t Save complete, going to sleep for about: {} [s]".format(sleep_time))
            time.sleep(sleep_time)
import os
import requests
import random
import time

from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content


def retrieve_and_save_file(row,
                           headers={'User-Agent': 'Individual Andres Oliva andresolivadenis@gmail.com',
                                    'Accept-Encoding': 'gzip, deflate',
                                    'Host': 'www.sec.gov'},
                           archive_base_url="https://www.sec.gov/Archives/",
                           path_default_files=os.path.dirname(
                               os.path.dirname(os.path.dirname(os.getcwd()))) + '/Data/raw/files',
                           verbose=True):
    """
    Retrieve and save archive files at SEC-EDGAR

    :param row:                             Index content row
    :param headers:                         EDGAR request header
    :param archive_base_url:                Base Url
    :param path_default_files:              Path to save retrieved file
    :param verbose:
    :return:   None
    """

    url = archive_base_url + row.Filename

    response = requests.get(url, headers=headers)

    if verbose:
        print("\t Retrieve Done, saving file ...")

    directory = os.path.join(path_default_files, os.path.join(str(row.year), row.quarter))
    directory = os.path.join(directory, os.path.join(row.CIK, row.Form_Type))
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_name = row.Filename.split("/")[-1]
    path_file = os.path.join(directory, file_name)

    with open(path_file, 'w') as f:
        f.write(response.text)


if __name__ == '__main__':

    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    companies_cik_list = ['320193', '1652044', '50863']
    year_list = list(range(2020, 2022, 1))
    quarter_lists = ['QTR4']  # ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    master_idx_contents = pre_process_master_idx_content(companies_cik_list=companies_cik_list,
                                                         year_list=year_list,
                                                         quarter_lists=quarter_lists,
                                                         path_out=None,
                                                         merged_file_name=None,
                                                         verbose=True)
    file_types_ = ['3', '4', '5']

    master_idx_contents_load = master_idx_contents.copy()
    if file_types_:
        master_idx_contents_load = master_idx_contents[master_idx_contents.Form_Type.isin(file_types_)].reset_index(
            drop=True)

    # ---------------------------------------------------------------------------------------------------------------

    for index, row_ in master_idx_contents_load.iterrows():
        print("\nRetrieving {}/{} ({:.2f}%)".format(index + 1, len(master_idx_contents_load),
                                                    100 * (index + 1) / len(master_idx_contents_load)))

        retrieve_and_save_file(row=row_,
                               verbose=True)

        sleep_interval = [0.5, 2]
        sleep_time = random.random() * (sleep_interval[1] - sleep_interval[0]) + sleep_interval[0]
        print("\t Save complete, going to sleep for about: {} [s]".format(sleep_time))
        time.sleep(sleep_time)

    # --------------------------------------------------------------------------------------------------------------------

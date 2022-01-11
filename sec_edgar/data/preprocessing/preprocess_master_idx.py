# https://www.sec.gov/os/accessing-edgar-dat

import os
import pandas as pd


def pre_process_master_idx_content(companies_cik_list,
                                   year_list,
                                   quarter_lists=['QTR1', 'QTR2', 'QTR3', 'QTR4'],
                                   archive_file="master.idx",
                                   path_out=None,
                                   merged_file_name=None,
                                   verbose=True,
                                   path_raw_default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))
                                                    + '/Data/raw/index'):
    """
    Preprocess Master index Files content

    :param companies_cik_list:              Companies cik codes list
    :param year_list:                       Years list
    :param quarter_lists:                   Quater list
    :param archive_file:                    master file name, default:
    :param path_out:                        Path out. Path to save results. None if not to save
    :param merged_file_name:                Merge file_name. None if not to save merge index content
    :param verbose:                         Print info in console
    :param path_raw_default:                Path of index files

    :return:
    """
    master_idx_content_dict_list = dict()
    for company in companies_cik_list:
        master_idx_content_dict_list[company] = []

    if verbose:
        print("\nPreprocessing archive_file: {}".format(archive_file))

    files_count = 0
    for year in year_list:
        year_str = str(year)
        for quarter in quarter_lists:
            files_count += 1
            if verbose:
                total = len(year_list) * len(quarter_lists)
                print("\tPreprocessing: {} / {} ({:.2f}%)".format(year_str, quarter, files_count/total*100))

            directory = os.path.join(path_raw_default, os.path.join(year_str, quarter))
            path_file = os.path.join(directory, archive_file)

            with open(path_file) as f:
                Lines = f.readlines()
                count = 0
                for line in Lines:
                    count += 1
                    line = line.replace("\n", "")
                    line_contents = line.split("|")
                    if len(line_contents) == 5:
                        for company in companies_cik_list:
                            if line_contents[0] == company:
                                master_idx_content_dict_list[company].append(dict(CIK=line_contents[0],
                                                                                  Company_Names=line_contents[1],
                                                                                  Form_Type=line_contents[2],
                                                                                  Date_Filed=line_contents[3],
                                                                                  Filename=line_contents[4],
                                                                                  year=year,
                                                                                  quarter=quarter))

    for company in companies_cik_list:
        master_idx_content_dict_list[company] = pd.DataFrame(master_idx_content_dict_list[company])

        if path_out is not None:
            file_name = company + " - " + master_idx_content_dict_list[company]['Company_Names'][0] + ".csv"

            if not os.path.exists(path_out):
                os.makedirs(path_out)

            master_idx_content_dict_list[company].to_csv(os.path.join(path_out, file_name), index=False)

    all_contents_df = pd.concat(master_idx_content_dict_list, ignore_index=True)

    if (merged_file_name is not None) and (path_out is not None):
        all_contents_df.to_csv(os.path.join(path_out, merged_file_name), index=False)

    if verbose:
        print("Preprocessing archive_file done\n")

    return all_contents_df


if __name__ == '__main__':

    companies_cik_list_ = ['320193', '1652044', '50863']
    companies_cik_list_ = ['50863']

    # year_list_ = list(range(1994, 2021, 1))
    year_list_ = list(range(2022, 2023, 1))
    quarter_lists_ = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    archive_file_ = "master.idx"
    merged_file_name_ = "Master_idx_content.csv"  # None if not to save

    path_out_ = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) + '/Data/pre_processed/company_files_list'

    master_idx_contents = pre_process_master_idx_content(companies_cik_list=companies_cik_list_,
                                                         year_list=year_list_,
                                                         quarter_lists=quarter_lists_,
                                                         path_out=None,
                                                         merged_file_name=None,
                                                         verbose=True)

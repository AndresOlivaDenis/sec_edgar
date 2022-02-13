# Split 4form_df by years
# Split 4form_df by cik grouping

# Split 4form_df by cik grouping

#       (random sub select might be good -> avoid selecting correlated sequences! line!
import os
import numpy as np
import pandas as pd


class DataSets4FormSplits(object):

    def __init__(self, processed_form4_df):
        self.processed_form4_df = processed_form4_df.copy()

    def split_by_cik_grouping(self, n_sub_set, append_all=True):
        companies_cik_list = self.processed_form4_df.CIK.unique().tolist()
        companies_cik_list_list = self.split_cik_list(companies_cik_list, n_sub_set)

        # form4_df_cik_grouping_list = [self.processed_form4_df[self.processed_form4_df.CIK.isin(companies_cik_list_)]
        #                               for companies_cik_list_ in companies_cik_list_list]

        ds_4form_grouping_dict = {
            "DS_cik_g{}".format(i): self.processed_form4_df[self.processed_form4_df.CIK.isin(companies_cik_list_)]
            for i, companies_cik_list_ in enumerate(companies_cik_list_list)}

        if append_all:
            ds_4form_grouping_dict["DS_all"] = self.processed_form4_df.copy()

        return ds_4form_grouping_dict

    def split_by_years(self, append_all=True):
        dates_index = pd.DatetimeIndex(self.processed_form4_df.Date_Filed)
        years = pd.unique(dates_index.year)

        # processed_form4_df_list = [self.processed_form4_df[dates_index.year.isin([year])]
        #                            for year in years]

        processed_form4_df_dict = {"DS_{}".format(year): self.processed_form4_df[dates_index.year.isin([year])]
                                   for year in years}
        if append_all:
            processed_form4_df_dict["DS_all"] = self.processed_form4_df.copy()
        return processed_form4_df_dict

    @staticmethod
    def split_cik_list(companies_cik_list, n_sub_set):
        splits_i = np.linspace(0, len(companies_cik_list), n_sub_set + 1).astype(int)
        companies_cik_list_list = [companies_cik_list[splits_i[i]:splits_i[i + 1]] for i in range(len(splits_i) - 1)]
        return companies_cik_list_list


if __name__ == '__main__':
    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + "us_market_allderv_shiftedDays_1019.py" + ".csv"
    processed_4form_df = pd.read_csv(file_name, index_col=0)
    # ----------------------------------------------------------------------------------------------------------------

    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    ds4fs = DataSets4FormSplits(processed_4form_df)
    # ----------------------------------------------------------------------------------------------------------------

    # split_by_cik_grouping ------------------------------------------------------------------------------------------
    form4_df_cik_grouping_dict_ = ds4fs.split_by_cik_grouping(n_sub_set=5)

    for name, form4_df_cik in form4_df_cik_grouping_dict_.items():
        print("{}, len: {}".format(name, len(form4_df_cik)))
    # ----------------------------------------------------------------------------------------------------------------

    # split by years -------------------------------------------------------------------------------------------------
    form4_df_years_dict_ = ds4fs.split_by_years()

    for name, form4_df_cik in form4_df_years_dict_.items():
        print("{}, len: {}".format(name, len(form4_df_cik)))
    # ----------------------------------------------------------------------------------------------------------------

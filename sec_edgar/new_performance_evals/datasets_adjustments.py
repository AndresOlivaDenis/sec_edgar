# Split 4form_df by years
# Split 4form_df by cik grouping

# Split 4form_df by cik grouping

#       (random sub select might be good -> avoid selecting correlated sequences! line!
import os
import numpy as np
import pandas as pd
import random


class DataSetsAdjustments(object):

    def __init__(self, processed_form4_df):
        self.processed_form4_df = processed_form4_df.copy()

    @staticmethod
    def limit_n_max_per_cik(processed_form4_df, n_max_per_cik=None, sort_by_date_column='Date_Filed'):
        processed_form4_df_n_max_per_cik = []
        if n_max_per_cik is None:
            cik_counts = processed_form4_df.groupby('CIK').CIK.count()
            n_max_per_cik = int(cik_counts.mean())

        for cik in pd.unique(processed_form4_df.CIK):
            cik_df = processed_form4_df[processed_form4_df.CIK == cik]
            cik_count = len(cik_df)
            if cik_count > n_max_per_cik:
                index_sub_set = random.sample(list(cik_df.index), n_max_per_cik)
                cik_df = cik_df.loc[index_sub_set]

            processed_form4_df_n_max_per_cik.append(cik_df)

        processed_form4_df_n_max_per_cik = pd.concat(processed_form4_df_n_max_per_cik, ignore_index=False)

        if sort_by_date_column:
            processed_form4_df_n_max_per_cik = processed_form4_df_n_max_per_cik.sort_values(by=sort_by_date_column)

        return processed_form4_df_n_max_per_cik


if __name__ == '__main__':
    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + "us_market_allderv_shiftedDays_1019.py" + ".csv"
    processed_4form_df_ = pd.read_csv(file_name, index_col=0)
    # ----------------------------------------------------------------------------------------------------------------

    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    processed_4form_df_adjusted = DataSetsAdjustments.limit_n_max_per_cik(processed_4form_df_, n_max_per_cik=None)
    # ----------------------------------------------------------------------------------------------------------------

import os
import numpy as np
import matplotlib.pyplot as plt
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest
import pandas as pd

from sec_edgar.utils.evaluation_metrics import PerformanceMetric01


class PerformanceEvaluations4Form(object):

    def __init__(self, processed_form4_df, n_sub_set):
        self.processed_form4_df = processed_form4_df.copy()
        self.n_sub_set = n_sub_set

        self.companies_cik_list = processed_form4_df.CIK.unique().tolist()
        self.companies_cik_list_list = self.split_cik_list(self.companies_cik_list, n_sub_set)
        self.processed_form4_df_list = self.split_processed_form4_df(self.processed_form4_df,
                                                                     self.companies_cik_list_list)

        self.data_sets_sizes = self.get_data_sets_sizes()

    def get_data_sets_sizes(self):
        data_sets_sizes = dict()
        label = "ss_{}".format("all")
        data_sets_sizes[label] = len(self.processed_form4_df)
        for i in range(self.n_sub_set):
            label = "ss_{}".format(i)
            data_sets_sizes[label] = len(self.processed_form4_df_list[i])
        return data_sets_sizes

    def eval_metric(self, performance_metric_ref, pm_kwargs):
        metrics_df = pd.DataFrame()
        metrics_objects_dict = dict()

        label = "ss_{}".format("all")
        metrics_objects_dict[label] = performance_metric_ref(**pm_kwargs)
        metrics_df[label] = metrics_objects_dict[label].eval(self.processed_form4_df)

        for i in range(self.n_sub_set):
            label = "ss_{}".format(i)
            metrics_objects_dict[label] = performance_metric_ref(**pm_kwargs)
            metrics_df[label] = metrics_objects_dict[label].eval(self.processed_form4_df_list[i])
        return metrics_df, metrics_objects_dict

    @staticmethod
    def split_cik_list(companies_cik_list, n_sub_set):
        splits_i = np.linspace(0, len(companies_cik_list), n_sub_set + 1).astype(int)
        companies_cik_list_list = [companies_cik_list[splits_i[i]:splits_i[i + 1]] for i in range(len(splits_i) - 1)]
        return companies_cik_list_list

    @staticmethod
    def split_processed_form4_df(processed_form4_df, companies_cik_list_list):
        processed_form4_df_list = [processed_form4_df[processed_form4_df.CIK.isin(companies_cik_list_)]
                                   for companies_cik_list_ in companies_cik_list_list]
        return processed_form4_df_list

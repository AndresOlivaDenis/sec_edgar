# master_idx_contents Inputs --------------------------------------------------------------------------------------
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

base_path = os.path.dirname(os.getcwd())

# Edgar Index content preprocessing ----------------------------------------------------------------------------------

path_company_ticket_file = base_path + "/Data/company_tickers.csv"
cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP', 'V', 'F', 'CVX', 'DAL', 'RCL', 'MSFT', 'FB', 'ADBE',
                         'PEP', 'CSCO', 'SBUX']

companies_symbol_list_ = ['TSLA', 'JPM', 'HD', 'UNH', 'JNJ', 'PG', 'BAC', 'PFE', 'MA', 'DIS', 'CMCSA', 'NKE',
                         'VZ', 'ABBV', 'KO']

companies_symbol_list_ = ['XOM', 'COST', 'QCOM', 'DHR', 'DHR', 'WMT', 'WFC', 'LLY', 'MCD', 'MRK', 'INTU', 'TXN',
                         'LOW', 'NEE', 'T', 'LIN', 'UNP', 'UPS', 'ORCL', 'MDT', 'MS', 'HON', 'PM']


companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
year_list = list(range(2015, 2019, 1))
quarter_lists = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
master_idx_contents_ = pre_process_master_idx_content(companies_cik_list=companies_cik_list,
                                                      year_list=year_list,
                                                      quarter_lists=quarter_lists,
                                                      path_out=None,
                                                      merged_file_name=None,
                                                      verbose=True,
                                                      path_raw_default=base_path + '/Data/raw/index')

# ---------------------------------------------------------------------------------------------------------------

# Edgar 4 form files retrieve and preprocessing -----------------------------------------------------------------

form4_df = pre_process_4form_archive_files(master_idx_contents=master_idx_contents_,
                                           path_default_files=base_path + '/Data/raw/files')

# ---------------------------------------------------------------------------------------------------------------

# Processing of 4form files: grouping and define of adjusted transactions ---------------------------------------

p4ff = Process4FormFiles(form4_df,
                         include_derivative_transaction=True,
                         sub_select_dict={'directOrIndirectOwnership': "D",
                                          'isDirector': '1'})

processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
processed_4form_df_ri_day = p4ff.get_transactions_by_day()

# ---------------------------------------------------------------------------------------------------------------


# Processing of 4form files: Append of Historical Data ------------------------------------------------------------

path_asset_historical_data = base_path + '/Data/asset_historical_data'
pahp = ProcessAppendHistoricalPrices(form4_df=form4_df,
                                     look_up_path=path_asset_historical_data,
                                     company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ = pahp.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ = pahp.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                    form4_df=processed_form4_df_)

pahp_ri = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri,
                                        look_up_path=path_asset_historical_data,
                                        company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ri = pahp_ri.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ri = pahp_ri.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                         form4_df=processed_form4_df_ri)

pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri_day,
                                            look_up_path=path_asset_historical_data,
                                            company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                                 form4_df=processed_form4_df_ri_day)

# ---------------------------------------------------------------------------------------------------------------

# TODO: define a funcion for this, and create a notebook


def eval_performance_metric_01(processed_form4_df,
                               label_eval_list = ['Price pct_change (5)', 'Price pct_change (21)',
                                                  'Price pct_change (63)', 'Price pct_change (126)',
                                                  'Price pct_change (252)']):
    """
    Evaluate processed_form4_df performance.
    Positive transactionSharesAdjust are evaluated under differents labels_eval.
    All transactions are considered as benchmark.
    Means are computed first by CIK grouping and them overall mean.

    Returns:
        performance_cik_df_dict -> dict of pd.DataFrame performance per label eval list
        labels_performance_df -> overall performance per label eval list
    """
    processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

    performance_cik_df_dict = dict()
    labels_performance_df = pd.DataFrame()

    for label_eval in label_eval_list:
        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')[label_eval].mean()
        performance_cik_df['benchmark'] = processed_form4_df_ri_day.groupby('CIK')[label_eval].mean()
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']
        performance_cik_df_dict[label_eval] = performance_cik_df.copy()

        performance_means_series = pd.Series(performance_cik_df.mean().rename({'is_greater': 'is_greater_prob'}),
                                             dtype='O')
        performance_means_series['is_pos_tse_ok'] = performance_means_series['Positive_tsa'] > performance_means_series[
            'benchmark']
        performance_means_series['is_greater_prob_ok'] = performance_means_series['is_greater_prob'] > 0.5
        performance_means_series['is_performance_ok'] = performance_means_series['is_pos_tse_ok'] and \
                                                        performance_means_series['is_greater_prob_ok']
        labels_performance_df[label_eval] = performance_means_series.copy()
    return labels_performance_df, performance_cik_df_dict


labels_performance_df_, performance_cik_df_dict_ = eval_performance_metric_01(processed_form4_df_ri_day)


def performance_metric_01(processed_form4_df, column_label_eval='Price pct_change (5)', return_performance_cik_df=True):
    """
    Evaluate processed_form4_df performance.
    Positive transactionSharesAdjust are evaluated under differents labels_eval.
    All transactions are considered as benchmark.
    Means are computed first by CIK grouping and them overall mean.

    Returns:
        performance_cik_df_dict -> dict of pd.DataFrame performance per label eval list
        labels_performance_df -> overall performance per label eval list
    """
    processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

    performance_cik_df = pd.DataFrame()
    performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')[column_label_eval].mean()
    performance_cik_df['benchmark'] = processed_form4_df_ri_day.groupby('CIK')[column_label_eval].mean()
    performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']

    performance_means_series = pd.Series(performance_cik_df.mean().rename({'is_greater': 'is_greater_prob'}),
                                         dtype='O')
    performance_means_series['is_pos_tse_ok'] = performance_means_series['Positive_tsa'] > performance_means_series[
        'benchmark']
    performance_means_series['is_greater_prob_ok'] = performance_means_series['is_greater_prob'] > 0.5
    performance_means_series['is_performance_ok'] = performance_means_series['is_pos_tse_ok'] and \
                                                    performance_means_series['is_greater_prob_ok']
    if return_performance_cik_df:
        return performance_means_series, performance_cik_df
    else:
        return performance_means_series

performance_means_series_, performance_cik_df_ = performance_metric_01(processed_form4_df_ri_day)

# TODO:
# - Create a class (or definition) that evaluates performance on differentes data sets
#       (recives an already processed processed_form4_df and them just split into differents ones)
#   - gets as inputs (after! On init just create data_sets!):
#       - Performance evluator (other class maybe -> returns a pd.Series containing evluation metric for a given processed_form4_df
#       - A list of companies_symbol_cik (list of lists ')
#   - Create method that splits a list of companies_symbol_cik on differents
#   - Alohgh maybe split later ? (eval also overall ?)


def split_cik_list(companies_cik_list, n_sub_set):
    splits_i = np.linspace(0, len(companies_cik_list), n_sub_set + 1).astype(int)
    companies_cik_list_list = [companies_cik_list[splits_i[i]:splits_i[i+1]] for i in range(len(splits_i) - 1)]
    return companies_cik_list_list


def split_processed_form4_df(processed_form4_df, companies_cik_list_list):
    processed_form4_df_list = [processed_form4_df[processed_form4_df.CIK.isin(companies_cik_list_)]
                               for companies_cik_list_ in companies_cik_list_list]
    return processed_form4_df_list



processed_form4_df_ri_day.CIK.isin(companies_cik_list[0:10])

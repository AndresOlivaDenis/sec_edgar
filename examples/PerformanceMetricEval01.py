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
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form

if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------

    base_path = os.path.dirname(os.getcwd())

    # Edgar Index content preprocessing ----------------------------------------------------------------------------------

    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

    companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP', 'V', 'F', 'CVX', 'DAL', 'RCL', 'MSFT', 'FB', 'ADBE',
                             'PEP', 'CSCO', 'SBUX']
    companies_symbol_list += ['TSLA', 'JPM', 'HD', 'UNH', 'JNJ', 'PG', 'BAC', 'PFE', 'MA', 'DIS', 'CMCSA', 'NKE',
                              'VZ', 'ABBV', 'KO']
    companies_symbol_list += ['XOM', 'COST', 'QCOM', 'DHR', 'DHR', 'WMT', 'WFC', 'LLY', 'MCD', 'MRK', 'INTU', 'TXN',
                              'LOW', 'NEE', 'T', 'LIN', 'UNP', 'UPS', 'ORCL', 'MDT', 'MS', 'HON', 'PM']

    path_symbols_file = base_path + "/Data/symbols/nasdaq_symbols.csv"
    # path_symbols_file = base_path + "/Data/symbols/dji_symbols.csv"
    path_symbols_file = base_path + "/Data/symbols/sp500_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list += symbols_file_df['Symbol'].to_list()
    companies_symbol_list = list(set(companies_symbol_list) - {'MTCH', 'COL', 'LB', 'MYL', 'ALXN', 'SCG', 'COG',
                                                               'VAR', 'XEC', 'TIF', 'FLIR'})

    companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
    year_list = [str(year) for year in range(2015, 2019, 1)]
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
    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 10, 21, 63, 126, 252],
                                                                     form4_df=processed_form4_df_ri_day)

    # ---------------------------------------------------------------------------------------------------------------

    pe_4form = PerformanceEvaluations4Form(processed_form4_df_ri_day, n_sub_set=3)
    metrics_df_, metrics_objects_dict_ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric01,
                                                              pm_kwargs=dict(column_label_eval='Price pct_change (5)'))
    print(metrics_objects_dict_)
    print(metrics_df_)


    # ------------------------------------------
    metrics_df_, metrics_objects_dict_ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric01,
                                                              pm_kwargs=dict(column_label_eval='Price pct_change (10)'))
    print(metrics_objects_dict_)
    print(metrics_df_)

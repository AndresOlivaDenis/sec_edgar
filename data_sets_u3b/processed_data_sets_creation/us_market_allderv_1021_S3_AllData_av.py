import os
import numpy as np
import matplotlib.pyplot as plt
from MT5PythonScriptsExperts.connectors.connector_one import ConnectorOne
from MT5PythonScriptsExperts.symbols_info.mt5_symbols_info_interface import StocksSymbolInfoInterface
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest
import pandas as pd

from sec_edgar.utils.evaluation_metrics import PerformanceMetric02
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form

from sec_edgar.data.processing.process_append_bench_historical_prices import \
    ProcessAppendBenchHistoricalPrices

from sec_edgar.real_time_data.real_time_stock_price_processing.real_time_stock_price_processing import \
    RealTimeMt5StockPriceProcessing

if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
    edgar_tickers_file_path = base_path + '/Data/company_tickers.csv'

    # Edgar Index content preprocessing ----------------------------------------------------------------------------------
    companies_symbol_list = []

    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

    # Connector initialization -------------------------------------------------------------------------------------
    info_msj = "\nInitializing MT5 terminal connector"
    print(info_msj)

    mt5_connector = ConnectorOne()
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    # Computing and processing admissible trading symbols -----------------------------------------------------------
    info_msj = "\nComputing and processing admissible trading symbols"
    print(info_msj)

    # CUrrent data ------------------------------------------------------------
    companies_cik_list = []
    raw_files_path = base_path + "/Data/raw/files/"
    for year in os.listdir(raw_files_path):
        year_path = raw_files_path + year + "/"
        for quarter in os.listdir(year_path):
            quarter_path = year_path + quarter + "/"
            companies_cik_list += os.listdir(quarter_path)
    companies_cik_list = list(set(companies_cik_list))

    # companies_cik_list Request historical data pre
    companies_cik_list_av_avialable, companies_cik_list_av_non_avialable = [], []
    for cik in companies_cik_list:
        try:
            AVHistoricalDataRequest(symbol=cik_mu.get_symbol_for_cik(cik),
                                    api_key="NIAI6K1QQ0KEXACB",
                                    look_up_path=base_path + '/Data/asset_historical_data',
                                    update_in_look_up_path=True)
            companies_cik_list_av_avialable.append(cik)
        except Exception as e:
            companies_cik_list_av_non_avialable.append(cik)

    companies_cik_list_ = companies_cik_list.copy()
    print("companies_cik_list_av_non_avialable: ", companies_cik_list_av_non_avialable)
    print(" {}/{} ".format(len(companies_cik_list_av_non_avialable), len(companies_cik_list_)))
    companies_cik_list = companies_cik_list_av_avialable.copy()

    # raise ValueError("Just testing")
    # ------------------------------------------------------------------------------

    year_list = [str(year) for year in range(2010, 2022, 1)]
    # year_list = [str(2021)]

    # year_list = [str(year) for year in range(2015, 2017, 1)]

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
                                               path_default_files=base_path + '/Data/raw/files',
                                               sleep_time_scale=0.25)

    # ---------------------------------------------------------------------------------------------------------------

    # Processing of 4form files: grouping and define of adjusted transactions ---------------------------------------
    p4ff = Process4FormFiles(form4_df,
                             include_derivative_transaction=True,
                             sub_select_dict={'directOrIndirectOwnership': "D"})

    # processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
    processed_4form_df = p4ff.get_transactions_by_day()
    # ---------------------------------------------------------------------------------------------------------------

    # Processing of 4form files: Append of Historical Data ------------------------------------------------------------
    path_asset_historical_data = base_path + '/Data/asset_historical_data'

    # Prices and pct_changes ahead: ------------------------------
    pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
                                                look_up_path=path_asset_historical_data,
                                                company_ticket_file_path=path_company_ticket_file)
    print("append_prices_shifted_ahead")
    processed_4form_df = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[5, 15],
                                                                 remove_non_available_symbols=False)
    print("append_pct_changes_ahead")
    processed_4form_df = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 10, 15],
                                                              form4_df=processed_4form_df,
                                                              remove_non_available_symbols=False)

    # Prices and pct_changes behind --------------------------------
    print("append_prices_shifted_behind")
    processed_4form_df = pahp_ri_day.append_prices_shifted_behind(periods_behind_list=[0, 5, 15],
                                                                  form4_df=processed_4form_df,
                                                                  remove_non_available_symbols=False)
    print("append_pct_changes_behind")
    processed_4form_df = pahp_ri_day.append_pct_changes_behind(periods_behind_list=[5, 10, 15, 21, 31, 42],
                                                               form4_df=processed_4form_df,
                                                               remove_non_available_symbols=True)

    # Bench pct_changes ----------------------------------------------
    path_yf_indices_historical_data = base_path + '/Data/yf_indices_historical_data'
    pahp_bench = ProcessAppendBenchHistoricalPrices(form4_df=processed_4form_df,
                                                    look_up_path=path_yf_indices_historical_data)
    processed_4form_df = pahp_bench.append_pct_changes_ahead(periods_ahead_list=[5, 10, 15, 21, 31, 42, 63, 126, 252],
                                                             form4_df=processed_4form_df)
    processed_4form_df = pahp_bench.append_pct_changes_behind(periods_behind_list=[5, 10, 15, 21, 31, 42, 63, 126, 252],
                                                              form4_df=processed_4form_df)
    # ---------------------------------------------------------------------------------------------------------------

    # Save of file ---------------------------------------------------------------------------------------------------
    path_processed_datasets = base_path + '/Data/processed_datasets_u3/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_4form_df.to_csv(file_name, index_label='index')
    # ----------------------------------------------------------------------------------------------------------------

    processed_4form_df_loaded = pd.read_csv(file_name, index_col=0)

    # TODO: create files (or notebooks) that loads each of the created datasets, and evaluates created metrics!
    #       Also include study per years results!

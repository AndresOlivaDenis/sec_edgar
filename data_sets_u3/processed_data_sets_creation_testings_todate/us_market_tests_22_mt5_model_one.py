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

from sec_edgar.data.postprocessing.postprocess_model_one import PostProcessPosChangeLast

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

    companies_symbol_list_e = []
    path_symbols_file = base_path + "/Data/symbols/nasdaq_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list_e += symbols_file_df['Symbol'].to_list()

    path_symbols_file = base_path + "/Data/symbols/dji_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list_e += symbols_file_df['Symbol'].to_list()

    path_symbols_file = base_path + "/Data/symbols/sp500_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list_e += symbols_file_df['Symbol'].to_list()

    companies_symbol_list_e = list(set(companies_symbol_list_e) - {'MTCH', 'KHC', 'PYPL', 'OKTA', 'DOCU', 'KO', 'MRNA',
                                                                   'MTCH', 'COL', 'LB', 'MYL', 'ALXN', 'SCG', 'COG',
                                                                   'VAR',
                                                                   'XEC', 'TIF', 'FLIR', 'CXO', 'UA', 'WLTW', 'UAA',
                                                                   'QRVO',
                                                                   'BKR', 'BHF'})
    companies_cik_list_e = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list_e]

    sci = StocksSymbolInfoInterface(mt5_connector)

    sci_admissible_symbols = sci.compute_admissible_symbols(relative_cost_threeshold=-0.05,
                                                            investing_balance=10000,
                                                            in_market_days=15,
                                                            cost_threeshold_in_years=True)

    symbol_name_list = list(sci_admissible_symbols.index)
    symbols_match_df = sci.match_symbols_name_with_edgar_cik_id(symbol_name_list=symbol_name_list,
                                                                edgar_tickers_file_path=edgar_tickers_file_path)

    companies_cik_list = symbols_match_df.cik.to_list()
    companies_cik_list = [cik for cik in companies_cik_list if cik in companies_cik_list_e]
    companies_cik_list = ['78003', '1364742', '1020569', '1555280', '1297996', '2488', '1341439']

    # year_list = [str(year) for year in range(2020, 2022, 1)]
    year_list = [str(2022)]

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
    mt5_connector_ = ConnectorOne()
    sci = StocksSymbolInfoInterface(mt5_terminal_connector=mt5_connector_)

    pahp_ri_day = RealTimeMt5StockPriceProcessing(form4_df=processed_4form_df.copy(),
                                                  mt5_connector=mt5_connector_,
                                                  symbols_info_interface=sci,
                                                 edgar_tickers_file_path=base_path + '/Data/company_tickers.csv')
    # pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
    #                                             look_up_path=path_asset_historical_data,
    #                                             company_ticket_file_path=path_company_ticket_file)

    # Prices and pct_changes behind --------------------------------
    print("append_prices_shifted_behind")
    processed_4form_df = pahp_ri_day.append_prices_shifted_behind(periods_behind_list=[0, 21],
                                                                  form4_df=processed_4form_df,
                                                                  remove_non_available_symbols=False)
    print("append_pct_changes_behind")
    processed_4form_df = pahp_ri_day.append_pct_changes_behind(periods_behind_list=[21],
                                                                 form4_df=processed_4form_df,
                                                               remove_non_available_symbols=False)

    print("append_prices_shifted_behind")
    processed_4form_df = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[15],
                                                                  form4_df=processed_4form_df,
                                                                  remove_non_available_symbols=False)

    print("append_prices_shifted_behind")
    processed_4form_df = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[15],
                                                                  form4_df=processed_4form_df,
                                                                  remove_non_available_symbols=False)


    # Post Processed --------------------------------------------------------------------------------------
    processed_form4_df_model = PostProcessPosChangeLast.post_process_df(processed_4form_df)

    # Save of file ---------------------------------------------------------------------------------------------------
    path_processed_datasets = base_path + '/Data/processed_datasets_u3/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_form4_df_model.to_csv(file_name, index_label='index')
    # ----------------------------------------------------------------------------------------------------------------

    # TODO: Validates:
    #   Check fillings (that is as expected, only 'A', ect...)
    #   Check prices ahead and behind! (as excel!)
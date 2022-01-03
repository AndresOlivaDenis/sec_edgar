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

from sec_edgar.utils.evaluation_metrics import PerformanceMetric02
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form

if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    # Edgar Index content preprocessing ----------------------------------------------------------------------------------
    companies_symbol_list = []

    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

    path_symbols_file = base_path + "/Data/symbols/nasdaq_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list += symbols_file_df['Symbol'].to_list()

    path_symbols_file = base_path + "/Data/symbols/dji_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list += symbols_file_df['Symbol'].to_list()

    path_symbols_file = base_path + "/Data/symbols/sp500_symbols.csv"
    symbols_file_df = pd.read_csv(path_symbols_file)
    companies_symbol_list += symbols_file_df['Symbol'].to_list()

    companies_symbol_list = list(set(companies_symbol_list) - {'MTCH', 'KHC', 'PYPL', 'OKTA', 'DOCU', 'KO', 'MRNA',
                                                               'MTCH', 'COL', 'LB', 'MYL', 'ALXN', 'SCG', 'COG', 'VAR',
                                                               'XEC', 'TIF', 'FLIR', 'CXO', 'UA', 'WLTW', 'UAA', 'QRVO',
                                                               'BKR', 'BHF'})

    companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
    year_list = [str(year) for year in range(2015, 2020, 1)]
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
                                               path_default_files=base_path + '/Data/raw/files')

    # ---------------------------------------------------------------------------------------------------------------

    # Processing of 4form files: grouping and define of adjusted transactions ---------------------------------------
    # TODO: In another file:
    #   Only Derivative transactions                        (1) -
    #   Only NonDerivative transactions                     (1) -
    p4ff = Process4FormFiles(form4_df,
                             include_derivative_transaction=False,
                             sub_select_dict={'directOrIndirectOwnership': "D"})

    # processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
    processed_4form_df = p4ff.get_transactions_by_day()

    # TODO: In another file:
    #   Fix! Review dates, some are wrong!  -> Create a def that prefix date! (date column as input) (remove dates below a ref min date?) (replace(-) -> int, min)
    #   date_delta = pd.Timedelta(days=1)                   (2) -
    #   date_delta = pd.Timedelta(days=2)                   (2) -
    #   date_delta = pd.Timedelta(days=3)                   (2) -

    # ---------------------------------------------------------------------------------------------------------------

    # Processing of 4form files: Append of Historical Data ------------------------------------------------------------
    path_asset_historical_data = base_path + '/Data/asset_historical_data'

    # Prices and pct_changes ahead: ------------------------------
    pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
                                                look_up_path=path_asset_historical_data,
                                                company_ticket_file_path=path_company_ticket_file)
    processed_4form_df = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
    processed_4form_df = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 10, 21, 63, 126, 252],
                                                              form4_df=processed_4form_df)

    # TODO: remove and create one without this
    # Prices and pct_changes ahead (shifted days) ------------------------------
    processed_4form_df = pahp_ri_day.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[21],
                                                                               dates_timedelta_list=[
                                                                                   pd.Timedelta("21 days"),
                                                                                   pd.Timedelta("-21 days")],
                                                                               form4_df=processed_4form_df)

    pahp_ri_day_sahd = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
                                                     look_up_path=path_asset_historical_data,
                                                     company_ticket_file_path=path_company_ticket_file)

    processed_4form_df = pahp_ri_day_sahd.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[5],
                                                                                    dates_timedelta_list=[
                                                                                        pd.Timedelta("5 days"),
                                                                                        pd.Timedelta("-5 days")],
                                                                                    form4_df=processed_4form_df)

    processed_4form_df = pahp_ri_day_sahd.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[10],
                                                                                    dates_timedelta_list=[
                                                                                        pd.Timedelta("10 days"),
                                                                                        pd.Timedelta("-10 days")],
                                                                                    form4_df=processed_4form_df)

    processed_4form_df = pahp_ri_day_sahd.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[15],
                                                                                    dates_timedelta_list=[
                                                                                        pd.Timedelta("15 days"),
                                                                                        pd.Timedelta(
                                                                                            "-15 days")],
                                                                                    form4_df=processed_4form_df)
    # ---------------------------------------------------------------------------------------------------------------

    # Save of file ---------------------------------------------------------------------------------------------------
    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_4form_df.to_csv(file_name, index_label='index')
    # ----------------------------------------------------------------------------------------------------------------

    processed_4form_df_loaded = pd.read_csv(file_name, index_col=0)

    # TODO: create files (or notebooks) that loads each of the created datasets, and evaluates created metrics!
    #       Also include study per years results!

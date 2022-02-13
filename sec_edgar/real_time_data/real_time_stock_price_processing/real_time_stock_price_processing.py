# Updated saved data, when the requested date is not far that 2days behind ?
#   Objective: add behind_price_column (pct_change) to 4form processed data frame.
#   Try to do it indepently from exsiting one? (in order them to make a better validation)

# Group by cik and get latest date.
#   Update files if:
#       Historical data doesnot exist or
#       If requested date is not in file
#       if Available - lasted_requested_date > 2Days (this date as input)


# SO:
#   First update existing files if applies
#   Them return the requested

import os
import MetaTrader5 as mt5
import random
import time

import numpy as np
import requests
import pandas as pd
from MT5PythonScriptsExperts.connectors.connector_one import ConnectorOne
from MT5PythonScriptsExperts.symbols_info.mt5_symbols_info_interface import StocksSymbolInfoInterface

from sec_edgar.real_time_data.real_time_stock_price_processing.rt_request_mt5 import RealTimeRequestMt5
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices

from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.processing.process_4form_files import Process4FormFiles


class RealTimeMt5StockPriceProcessing(ProcessAppendHistoricalPrices):

    def __init__(self, form4_df, mt5_connector, symbols_info_interface, edgar_tickers_file_path,
                 cik_column='CIK', date_column='Date_Filed'):
        self.mt5_connector = mt5_connector
        self.form4_df_0 = form4_df.copy()
        self.cik_column = cik_column
        self.date_column = date_column

        self.cik_series = pd.unique(self.form4_df_0[self.cik_column])
        self.sci = symbols_info_interface

        self.hdr_dict, self.cik_dates = dict(), dict()
        for cik in self.cik_series:
            self.hdr_dict[cik] = RealTimeRequestMt5(symbol=self.sci.get_symbol_for_cik(cik,
                                                                                       edgar_tickers_file_path),
                                                    mt5_connector=mt5_connector)
            self.cik_dates[cik] = self.form4_df_0.loc[self.form4_df_0[cik_column] == cik, date_column]


if __name__ == '__main__':
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))
    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP']
    companies_symbol_list = ['JPM', 'HD']  # , 'UNH', 'JNJ', 'PG', 'BAC']
    companies_symbol_list = ['HD']  # , 'UNH', 'JNJ', 'PG', 'BAC']
    companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP', 'HPE', 'BHF']
    # companies_symbol_list = ['AAL']

    companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    # year_list = list(range(2018, 2022, 1))
    # year_list = list(range(2015, 2019, 1))
    year_list = list(range(2019, 2022, 1))
    quarter_lists = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    master_idx_contents_ = pre_process_master_idx_content(companies_cik_list=companies_cik_list,
                                                          year_list=year_list,
                                                          quarter_lists=quarter_lists,
                                                          path_out=None,
                                                          merged_file_name=None,
                                                          verbose=True)

    # ---------------------------------------------------------------------------------------------------------------

    # form4_df_f = form4_df_f[form4_df_f.transactionPricePerShare.isna().__neg__()]
    # form4_df_f = form4_df_f[form4_df_f.transactionPricePerShare.astype(np.float) > 0]
    # form4_df_f = form4_df_f.dropna(axis=1, how='all')

    # Begining of main_def
    form4_df_ = pre_process_4form_archive_files(master_idx_contents=master_idx_contents_)

    pahp = ProcessAppendHistoricalPrices(form4_df=form4_df_)
    processed_form4_df_ = pahp.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
    processed_form4_df_ = pahp.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                        form4_df=processed_form4_df_)

    p4ff = Process4FormFiles(form4_df_, include_derivative_transaction=False)
    processed_4form_df_ri_day = p4ff.get_transactions_by_day()

    pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri_day)
    processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[15, 21, 42])
    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 15, 21, 42],
                                                                     form4_df=processed_form4_df_ri_day)

    processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_behind(periods_behind_list=[5, 15, 21, 42],
                                                                         form4_df=processed_form4_df_ri_day)

    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_behind(periods_behind_list=[5, 15, 21, 42],
                                                                     form4_df=processed_form4_df_ri_day)

    # -------------------------------------------------------------------------------------------------------------

    mt5_connector_ = ConnectorOne()
    sci = StocksSymbolInfoInterface(mt5_terminal_connector=mt5_connector_)

    processed_4form_df_ri_day_mt5 = p4ff.get_transactions_by_day()

    pahp_mt5_ri_day = RealTimeMt5StockPriceProcessing(processed_4form_df_ri_day_mt5, mt5_connector_, sci,
                                                      os.getcwd() + '/resources/company_tickers.csv')

    processed_form4_df_ri_day_mt5 = pahp_mt5_ri_day.append_prices_shifted_ahead(periods_ahead_list=[15, 21, 42])
    processed_form4_df_ri_day_mt5 = pahp_mt5_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 15, 21, 42],
                                                                     form4_df=processed_form4_df_ri_day_mt5)

    processed_form4_df_ri_day_mt5 = pahp_mt5_ri_day.append_prices_shifted_behind(periods_behind_list=[5, 15, 21, 42],
                                                                         form4_df=processed_form4_df_ri_day_mt5)

    processed_form4_df_ri_day_mt5 = pahp_mt5_ri_day.append_pct_changes_behind(periods_behind_list=[5, 15, 21, 42],
                                                                     form4_df=processed_form4_df_ri_day_mt5)

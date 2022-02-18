import os
import time
import random
import requests
from datetime import datetime

import logging
import warnings
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from MT5PythonScriptsExperts.connectors.connector_one import ConnectorOne
from MT5PythonScriptsExperts.symbols_info.mt5_symbols_info_interface import StocksSymbolInfoInterface

from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil

from sec_edgar.real_time_data.real_time_4form_processing import RealTime4FormProcessing

from sec_edgar.real_time_data.real_time_stock_price_processing.real_time_stock_price_processing import \
    RealTimeMt5StockPriceProcessing

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class RealTime4FormProcessingModelOne(RealTime4FormProcessing):
    def __init__(self, cik_list,
                 mt5_connector,
                 symbols_info_interface,
                 period_behind,
                 upper_interval,
                 edgar_tickers_file_path,
                 data_path=os.path.dirname(os.path.dirname(
                     os.path.dirname(os.getcwd()))) + '/real_time_data/',
                 start_date_time_delta=pd.to_timedelta('90 days'), sleep_time_scale=0.25,
                 verbose=True):
        self.cik_list = cik_list
        self.data_path = data_path
        self.sleep_time_scale = sleep_time_scale
        self.start_date = datetime.now() - start_date_time_delta
        self.mt5_connector = mt5_connector
        self.sci = symbols_info_interface
        self.period_behind = period_behind
        self.upper_interval = upper_interval
        self.behind_price_column = 'Price pct_change (-{})'.format(period_behind)

        # Retrieve of recent index fillings
        if verbose:
            msj = "Retrieving recent index fillings " + "-" * 40
            print(msj)

        self.form_index_fillings_df = pd.concat([
            self.retrieve_recent_4form_index_fillings_df(cik=cik,
                                                         sleep_time_scale=sleep_time_scale,
                                                         verbose=verbose,
                                                         start_date=self.start_date,
                                                         end_date=None) for cik in cik_list], ignore_index=False)

        if verbose:
            msj = "-" * 103 + "\n"
            msj += "Retrieving and preprocesing 4form archive files " + "-" * 55
            print(msj)

        self.pre_processed_form4_df = self.pre_process_4form_archive_files(self.form_index_fillings_df.copy(),
                                                                           data_path=data_path,
                                                                           sleep_time_scale=sleep_time_scale)

        if verbose:
            msj = "-" * 103 + "\n"
            msj += "Processing 4form archive files " + "-" * 55
            print(msj)

        self.processed_form4_df = self.process_4form_archive_files(self.pre_processed_form4_df.copy())

        if verbose:
            msj = "-" * 103 + "\n"
            msj += "Post-Processing 4form archive files " + "-" * 55
            print(msj)

        # edgar_tickers_file_path = data_path + 'company_tickers.csv'
        # self.post_processed_form4_df = self.post_process_4form_archive_files(self.processed_form4_df.copy())
        self.post_processed_form4_df = self.post_process_pct_change_behind_4form_archive_files(
            self.processed_form4_df.copy(),
            mt5_connector,
            symbols_info_interface,
            edgar_tickers_file_path,
            period_behind,
            upper_interval
        )

    @staticmethod
    def post_process_4form_archive_files(processed_4form_df):
        """
        Include here future improvements
        """
        raise ValueError("Please call post_process_pct_change_behind_4form_archive_files")

    @staticmethod
    def post_process_pct_change_behind_4form_archive_files(processed_4form_df, mt5_connector, symbols_info_interface,
                                                           edgar_tickers_file_path, period_behind, upper_interval):
        """
        Include here future improvements
        """
        post_processed_form4_df = processed_4form_df.copy()
        post_processed_form4_df = post_processed_form4_df[post_processed_form4_df.my_derivative_types.isin(['A'])]
        post_processed_form4_df = post_processed_form4_df[post_processed_form4_df['transactionSharesAdjust'] > 0]

        rt_mt5_spp = RealTimeMt5StockPriceProcessing(post_processed_form4_df,
                                                     mt5_connector,
                                                     symbols_info_interface,
                                                     edgar_tickers_file_path)
        post_processed_form4_df = rt_mt5_spp.append_pct_changes_behind(periods_behind_list=[period_behind],
                                                                       form4_df=post_processed_form4_df,
                                                                       remove_non_available_symbols=True)

        if upper_interval > 0:
            raise ValueError("Expected an upper_interval > 0")  # TO remember and do not make any pitfall!!
        behind_price_column = 'Price pct_change (-{})'.format(period_behind)
        post_processed_form4_df = post_processed_form4_df[post_processed_form4_df[behind_price_column] < upper_interval]
        return post_processed_form4_df

    def get_latest_positive_pct_change_behind_dict(self):
        latest_positive_pct_change_behind_dict = dict()
        for cik, max_date in self.get_latest_positive_transaction_day_dict().items():
            lppcb = self.post_processed_form4_df.copy()
            lppcb = lppcb[lppcb.CIK == cik]
            lppcb = lppcb[lppcb.Date_Filed == max_date]
            latest_positive_pct_change_behind_dict[cik] = lppcb.iloc[0][self.behind_price_column]
        return latest_positive_pct_change_behind_dict

    def get_latest_positive_pct_change_behind_for_cik(self, cik):
        latest_positive_pct_change_behind_dict = self.get_latest_positive_pct_change_behind_dict()
        if cik in latest_positive_pct_change_behind_dict.keys():
            return latest_positive_pct_change_behind_dict[cik]
        return None

    def get_day_positive_transactions_cik_list(self, date_to_eval):
        day_positive_transactions_cik_list = [cik for cik, day in
                                              self.get_latest_positive_transaction_day_dict().items()
                                              if day.strftime('%Y_%m_%d') == date_to_eval.strftime('%Y_%m_%d')]

        if day_positive_transactions_cik_list:
            pct_change_behind_list = [self.get_latest_positive_pct_change_behind_for_cik(cik)
                                      for cik in day_positive_transactions_cik_list]
            dpt_series = pd.Series(data=pct_change_behind_list, index=day_positive_transactions_cik_list)
            day_positive_transactions_cik_list = list(dpt_series.sort_values(ascending=True).index)

        return day_positive_transactions_cik_list


if __name__ == '__main__':
    mt5_connector_ = ConnectorOne()
    sci = StocksSymbolInfoInterface(mt5_terminal_connector=mt5_connector_)

    companies_cik_list = ['320193', '1652044', '50863', '40729']
    companies_cik_list = ['78003', '1364742', '1020569', '1555280', '1297996', '2488', '1341439']

    rt4fp = RealTime4FormProcessingModelOne(cik_list=companies_cik_list,
                                            mt5_connector=mt5_connector_,
                                            symbols_info_interface=sci,
                                            period_behind=21,
                                            upper_interval=-0.05,
                                            edgar_tickers_file_path=os.path.dirname(os.path.dirname(
                     os.path.dirname(os.getcwd()))) + '/real_time_data/' + 'company_tickers.csv')

    print("rt4fp.get_latest_positive_transaction_day_dict(): \n", rt4fp.get_latest_positive_transaction_day_dict())
    print("rt4fp.get_today_positive_transactions_cik_list(): \n", rt4fp.get_today_positive_transactions_cik_list())
    latest_positive_transaction_day_dict__ = rt4fp.get_latest_positive_transaction_day_dict()

    print('day_positive_transactions_cik_list(pd.to_datetime("2022-02-19"): ',
          rt4fp.get_day_positive_transactions_cik_list(pd.to_datetime("2022-01-19")))

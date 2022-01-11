import os
import time
import random
import requests
from datetime import datetime

import logging
import warnings
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files

from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(__name__)


class RealTime4FormProcessing(object):
    def __init__(self, cik_list,
                 data_path=os.path.dirname(os.path.dirname(
                     os.path.dirname(os.getcwd()))) + '/real_time_data/',
                 start_date_time_delta=pd.to_timedelta('90 days'), sleep_time_scale=0.25,
                 verbose=True):
        self.cik_list = cik_list
        self.data_path = data_path
        self.sleep_time_scale = sleep_time_scale
        self.start_date = datetime.now() - start_date_time_delta

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

        self.post_processed_form4_df = self.post_process_4form_archive_files(self.processed_form4_df.copy())

        # Retrieve retrieve_cik_submissions:
        #   -  concat retrieve_recent_4form_index_fillings_df results df
        #   -         #   Check for len > 0 (only concat if!)

        # preprocess 4form files
        # processs 4from files
        # post-process 4form files
        pass

    @staticmethod
    def retrieve_recent_4form_index_fillings_df(cik, sleep_time_scale=1.0, verbose=True,
                                                start_date=None, end_date=None):
        request_url = "https://data.sec.gov/submissions/CIK{}.json".format(cik.zfill(10))
        headers = {'User-Agent': 'Individual Andres Oliva andresolivadenis@gmail.com'}

        if verbose:
            msj = "\n Retrieving submissions for cik: {} (url: {})".format(cik, request_url)
            print(msj)
            # log.info(msj)

        response = requests.get(request_url, headers=headers)
        json_content = response.json()
        fillings_dict = json_content['filings']
        recent_fillings_df = pd.DataFrame(fillings_dict['recent'])

        recent_4form_index_fillings_df = recent_fillings_df[recent_fillings_df.form == '4'].copy()
        recent_4form_index_fillings_df['CIK'] = cik
        recent_4form_index_fillings_df['Filename'] = "edgar/data/{}/".format(cik) + recent_4form_index_fillings_df[
            'accessionNumber'] + ".txt"

        dates_columns = ['acceptanceDateTime', 'filingDate', 'reportDate']
        for date_column in dates_columns:
            recent_4form_index_fillings_df[date_column] = pd.to_datetime(recent_4form_index_fillings_df[date_column])

        sleep_interval = [0.5, 2]
        sleep_time = random.random() * (sleep_interval[1] - sleep_interval[0]) + sleep_interval[0]
        sleep_time = sleep_time * sleep_time_scale
        if verbose:
            msj = "\t retrieve complete, going to sleep for about: {} [s]".format(sleep_time)
            print(msj)
            # log.info(msj)

            time.sleep(sleep_time)

        recent_4form_index_fillings_df['Form_Type'] = recent_4form_index_fillings_df['form']
        recent_4form_index_fillings_df['Date_Filed'] = recent_4form_index_fillings_df['filingDate']

        recent_4form_index_fillings_df['Company_Names'] = json_content['name']
        recent_4form_index_fillings_df['tickers'] = str(json_content['tickers'])
        # recent_4form_index_fillings_df['Symbol'] = cik_mu.get_symbol_for_cik(cik)

        columns_to_drop = ['act', 'fileNumber', 'filmNumber', 'items', 'size', 'isXBRL', 'isInlineXBRL',
                           'primaryDocument', 'primaryDocDescription']
        recent_4form_index_fillings_df = recent_4form_index_fillings_df.drop(columns=columns_to_drop)

        if start_date is not None:
            recent_4form_index_fillings_df = recent_4form_index_fillings_df[
                recent_4form_index_fillings_df.reportDate >= start_date]
        if end_date is not None:
            recent_4form_index_fillings_df = recent_4form_index_fillings_df[
                recent_4form_index_fillings_df.reportDate <= end_date]

        return recent_4form_index_fillings_df.copy()

    @staticmethod
    def pre_process_4form_archive_files(form4_index_fillings_df,
                                        data_path=os.path.dirname(os.path.dirname(
                                            os.path.dirname(os.getcwd()))) + '/real_time_data/',
                                        sleep_time_scale=0.5):

        return pre_process_4form_archive_files(master_idx_contents=form4_index_fillings_df.copy(),
                                               path_default_files=data_path,
                                               include_year_quarter_in_path=False,
                                               sleep_time_scale=sleep_time_scale)

    @staticmethod
    def process_4form_archive_files(form4_df):
        p4ff = Process4FormFiles(form4_df.copy(),
                                 include_derivative_transaction=True,
                                 sub_select_dict={'directOrIndirectOwnership': "D"})

        # processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
        return p4ff.get_transactions_by_day()

    @staticmethod
    def post_process_4form_archive_files(processed_4form_df):
        post_processed_form4_df = processed_4form_df.copy()
        post_processed_form4_df = post_processed_form4_df[post_processed_form4_df.my_derivative_types.isin(['AB', 'B'])]
        post_processed_form4_df = post_processed_form4_df[post_processed_form4_df['transactionSharesAdjust'] > 0]
        return post_processed_form4_df

    def _get_latest_transaction_day_dict(self):
        p_processed_form4_df = self.processed_form4_df[self.processed_form4_df.my_derivative_types.isin(['AB', 'B'])]
        return p_processed_form4_df.groupby(['CIK']).Date_Filed.max().to_dict()

    def _get_today_transactions_cik_list(self):
        return [cik for cik, day in self._get_latest_transaction_day_dict().items()
                if day.strftime('%Y_%m_%d') == datetime.today().strftime('%Y_%m_%d')]

    def get_latest_positive_transaction_day_dict(self):
        return self.post_processed_form4_df.groupby(['CIK']).Date_Filed.max().to_dict()

    def get_latest_positive_transaction_day_for_cik(self, cik):
        latest_positive_transaction_day_dict = self.get_latest_positive_transaction_day_dict()
        if cik in latest_positive_transaction_day_dict.keys():
            return latest_positive_transaction_day_dict[cik]
        return None

    def get_today_positive_transactions_cik_list(self):
        return [cik for cik, day in self.get_latest_positive_transaction_day_dict().items()
                if day.strftime('%Y_%m_%d') == datetime.today().strftime('%Y_%m_%d')]


if __name__ == '__main__':
    companies_cik_list = ['320193', '1652044', '50863', '40729']

    rt4fp = RealTime4FormProcessing(cik_list=companies_cik_list)
    form_index_fillings_df = rt4fp.form_index_fillings_df

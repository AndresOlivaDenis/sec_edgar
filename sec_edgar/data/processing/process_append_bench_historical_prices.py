import os
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest

from sec_edgar.stock_historical_data.av_historical_data_request import NoAvailableDate

path_yf_indices_historical_data = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) + '/Data/yf_indices_historical_data'

bench_reference_symbol = "GSPC"


class ProcessAppendBenchHistoricalPrices(object):

    def __init__(self, form4_df,
                 cik_column='CIK', date_column='Date_Filed',
                 bench_reference_symbol=bench_reference_symbol,
                 api_key="NIAI6K1QQ0KEXACB",
                 look_up_path=path_yf_indices_historical_data):

        self.form4_df_0 = form4_df.copy()
        self.cik_column = cik_column
        self.date_column = date_column

        self.cik_series = pd.unique(self.form4_df_0[self.cik_column])
        self.bench_reference_symbol = bench_reference_symbol

        self.bench_hdr = AVHistoricalDataRequest(symbol=bench_reference_symbol,
                                                 api_key=api_key,
                                                 look_up_path=look_up_path,
                                                 update_in_look_up_path=True)

        # self.hdr_dict, self.cik_dates = dict(), dict()
        self.cik_dates = dict()
        for cik in self.cik_series:
            # self.hdr_dict[cik] = AVHistoricalDataRequest(symbol=self.cik_mu.get_symbol_for_cik(cik),
            #                                              api_key=api_key,
            #                                              look_up_path=look_up_path,
            #                                             update_in_look_up_path=True)
            self.cik_dates[cik] = self.form4_df_0.loc[self.form4_df_0[cik_column] == cik, date_column]

    def append_prices_shifted_ahead(self, periods_ahead_list, form4_df=None, remove_non_available_symbols=True):
        if form4_df is not None:
            processed_form4_df = form4_df.copy()
        else:
            processed_form4_df = self.form4_df_0.copy()

        label_dict_astype = {}
        for period_ahead in periods_ahead_list:
            label = "{} Shifted Price ({})".format(self.bench_reference_symbol, period_ahead)
            label_dict_astype[label] = float
            processed_form4_df[label] = None

        no_avilable_dates_cik = []
        for cik in self.cik_series:
            try:
                for period_ahead in periods_ahead_list:
                    label = "{} Shifted Price ({})".format(self.bench_reference_symbol, period_ahead)
                    sas = self.bench_hdr.get_shifted_ahead_series(dates=self.cik_dates[cik],
                                                                  periods_ahead=period_ahead,
                                                                  name=label)
                    processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
            except NoAvailableDate as e:
                no_avilable_dates_cik.append(cik)
        if no_avilable_dates_cik:
            print("\tWARNING, No avilable dates for the following symbols: ", no_avilable_dates_cik)
            if remove_non_available_symbols:
                print("\t\t Symbols have been removed")
                is_in = processed_form4_df[self.cik_column].isin(no_avilable_dates_cik)
                processed_form4_df = processed_form4_df[~is_in]

        processed_form4_df = processed_form4_df.astype(label_dict_astype)
        return processed_form4_df.copy()

    def append_pct_changes_ahead(self, periods_ahead_list, form4_df=None, remove_non_available_symbols=True):
        processed_form4_df = self.form4_df_0.copy()
        if form4_df is not None:
            processed_form4_df = form4_df.copy()

        for period_ahead in periods_ahead_list:
            label = "{} Price pct_change ({})".format(self.bench_reference_symbol, period_ahead)
            processed_form4_df[label] = None

        no_avilable_dates_cik = []
        for cik in self.cik_series:
            try:
                for period_ahead in periods_ahead_list:
                    label = "{} Price pct_change ({})".format(self.bench_reference_symbol, period_ahead)
                    sas = self.bench_hdr.get_pct_change_ahead_series(dates=self.cik_dates[cik],
                                                                     periods_ahead=period_ahead,
                                                                     name=label)
                    processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
            except NoAvailableDate as e:
                no_avilable_dates_cik.append(cik)
        if no_avilable_dates_cik:
            print("\tWARNING, No avilable dates for the following symbols: ", no_avilable_dates_cik)
            if remove_non_available_symbols:
                print("\t\t Symbols have been removed")
                is_in = processed_form4_df[self.cik_column].isin(no_avilable_dates_cik)
                processed_form4_df = processed_form4_df[~is_in]
        return processed_form4_df.copy()

    def append_pct_changes_behind(self, periods_behind_list, form4_df=None, remove_non_available_symbols=True):
        processed_form4_df = self.form4_df_0.copy()
        if form4_df is not None:
            processed_form4_df = form4_df.copy()

        for period_behind in periods_behind_list:
            label = "{} Price pct_change (-{})".format(self.bench_reference_symbol, abs(period_behind))
            processed_form4_df[label] = None

        no_avilable_dates_cik = []
        for cik in self.cik_series:
            try:
                for period_behind in periods_behind_list:
                    label = "{} Price pct_change (-{})".format(self.bench_reference_symbol, abs(period_behind))
                    sas = self.bench_hdr.get_pct_change_behind_series(dates=self.cik_dates[cik],
                                                                          periods_behind=period_behind,
                                                                          name=label)
                    processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
            except NoAvailableDate as e:
                no_avilable_dates_cik.append(cik)
        if no_avilable_dates_cik:
            print("\tWARNING, No avilable dates for the following symbols: ", no_avilable_dates_cik)
            if remove_non_available_symbols:
                print("\t\t Symbols have been removed")
                is_in = processed_form4_df[self.cik_column].isin(no_avilable_dates_cik)
                processed_form4_df = processed_form4_df[~is_in]
        return processed_form4_df.copy()

    def _append_pct_changes_ahead_in_shifted_dates(self, periods_ahead_list, dates_timedelta_list, form4_df=None,
                                                   remove_non_available_symbols=True):
        """
        dates_shift -> pd.Timedelta, ie: pd.Timedelta("1 days")
        """
        processed_form4_df = self.form4_df_0.copy()
        if form4_df is not None:
            processed_form4_df = form4_df.copy()

        for period_ahead in periods_ahead_list:
            for dates_timedelta in dates_timedelta_list:
                label = "{} Shifted Price pct_change ({})({})".format(self.bench_reference_symbol, period_ahead,
                                                                      dates_timedelta)
                processed_form4_df[label] = None

        no_avilable_dates_cik = []
        for cik in self.cik_series:
            try:
                for period_ahead in periods_ahead_list:
                    for dates_timedelta in dates_timedelta_list:
                        dates = self.cik_dates[cik] + dates_timedelta
                        label = "{} Shifted Price pct_change ({})({})".format(self.bench_reference_symbol, period_ahead,
                                                                              dates_timedelta)
                        sas = self.bench_hdr.get_pct_change_ahead_series(dates=dates,
                                                                         periods_ahead=period_ahead,
                                                                         name=label)
                        processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
            except NoAvailableDate as e:
                no_avilable_dates_cik.append(cik)
        if no_avilable_dates_cik:
            print("\tWARNING, No avilable dates for the following symbols: ", no_avilable_dates_cik)
            if remove_non_available_symbols:
                print("\t\t Symbols have been removed")
                is_in = processed_form4_df[self.cik_column].isin(no_avilable_dates_cik)
                processed_form4_df = processed_form4_df[~is_in]
        return processed_form4_df.copy()


if __name__ == '__main__':
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))
    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP']
    companies_symbol_list = ['JPM', 'HD']  # , 'UNH', 'JNJ', 'PG', 'BAC']
    companies_symbol_list = ['HD']  # , 'UNH', 'JNJ', 'PG', 'BAC']
    companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP', 'HPE', 'BHF']

    companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    # year_list = list(range(2018, 2022, 1))
    year_list = list(range(2015, 2019, 1))
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

    pahp = ProcessAppendBenchHistoricalPrices(form4_df=form4_df_)
    processed_form4_df_ = pahp.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
    processed_form4_df_ = pahp.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                        form4_df=processed_form4_df_)

    p4ff = Process4FormFiles(form4_df_, include_derivative_transaction=False)
    processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
    processed_4form_df_ri_day = p4ff.get_transactions_by_day()

    pahp_ri_day = ProcessAppendBenchHistoricalPrices(form4_df=processed_4form_df_ri_day)
    processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20],
                                                                        remove_non_available_symbols=False)
    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                              form4_df=processed_form4_df_ri_day,
                                                              remove_non_available_symbols=False)

    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_behind(periods_behind_list=[0, 5, 20],
                                                                      form4_df=processed_form4_df_ri_day)

    # Tests: (To mov

    # TODO:
    #   Notebook to "study, check, validate and get ideas from data)
    #      in datetime plots (several subplots, date in x):
    #           Shares / (% in other graph) Histograms
    #           Price ahead (linear plot)
    #           pct_change (histograms)

    #   Validate:
    #       Plot historical prices (direct from alphavantage)
    #       and also macthed dates - prices from here!!

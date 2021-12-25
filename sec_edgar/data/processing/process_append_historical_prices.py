import os
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest

path_asset_historical_data = os.path.dirname(
    os.path.dirname(os.path.dirname(os.getcwd()))) + '/Data/asset_historical_data'

path_company_ticket_file = os.path.dirname(
    os.path.dirname(os.path.dirname(os.getcwd()))) + '/Data/company_tickers.csv'


class ProcessAppendHistoricalPrices(object):

    def __init__(self, form4_df,
                 cik_column='CIK', date_column='Date_Filed',
                 api_key="NIAI6K1QQ0KEXACB", look_up_path=path_asset_historical_data,
                 company_ticket_file_path=path_company_ticket_file):

        self.form4_df_0 = form4_df.copy()
        self.cik_column = cik_column
        self.date_column = date_column

        self.cik_mu = CikMappingUtil(company_ticket_file_path=company_ticket_file_path)
        self.cik_series = pd.unique(self.form4_df_0[self.cik_column])

        self.hdr_dict, self.cik_dates = dict(), dict()
        for cik in self.cik_series:
            self.hdr_dict[cik] = AVHistoricalDataRequest(symbol=self.cik_mu.get_symbol_for_cik(cik),
                                                         api_key=api_key,
                                                         look_up_path=look_up_path,
                                                         update_in_look_up_path=True)
            self.cik_dates[cik] = self.form4_df_0.loc[self.form4_df_0[cik_column] == cik, date_column]

    def append_prices_shifted_ahead(self, periods_ahead_list, form4_df=None):
        if form4_df is not None:
            processed_form4_df = form4_df.copy()
        else:
            processed_form4_df = self.form4_df_0.copy()

        label_dict_astype = {}
        for period_ahead in periods_ahead_list:
            label = "Shifted Price ({})".format(period_ahead)
            label_dict_astype[label] = float
            processed_form4_df[label] = None

        for cik in self.cik_series:
            for period_ahead in periods_ahead_list:
                label = "Shifted Price ({})".format(period_ahead)
                sas = self.hdr_dict[cik].get_shifted_ahead_series(dates=self.cik_dates[cik],
                                                                  periods_ahead=period_ahead,
                                                                  name=label)
                processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
        processed_form4_df = processed_form4_df.astype(label_dict_astype)
        return processed_form4_df.copy()

    def append_pct_changes_ahead(self, periods_ahead_list, form4_df=None):
        processed_form4_df = self.form4_df_0.copy()
        if form4_df is not None:
            processed_form4_df = form4_df.copy()

        for period_ahead in periods_ahead_list:
            label = "Price pct_change ({})".format(period_ahead)
            processed_form4_df[label] = None

        for cik in self.cik_series:
            for period_ahead in periods_ahead_list:
                label = "Price pct_change ({})".format(period_ahead)
                sas = self.hdr_dict[cik].get_pct_change_ahead_series(dates=self.cik_dates[cik],
                                                                     periods_ahead=period_ahead,
                                                                     name=label)
                processed_form4_df.loc[processed_form4_df[self.cik_column] == cik, label] = sas.copy().values
        return processed_form4_df.copy()


if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    companies_cik_list = ['320193', '1652044', '50863']
    year_list = list(range(2018, 2022, 1))
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
    processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
    processed_4form_df_ri_day = p4ff.get_transactions_by_day()

    pahp_ri = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri)
    processed_form4_df_ri = pahp_ri.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
    processed_form4_df_ri = pahp_ri.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                             form4_df=processed_form4_df_ri)

    pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri_day)
    processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
    processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                             form4_df=processed_form4_df_ri_day)

    p4ff_2 = Process4FormFiles(processed_form4_df_, include_derivative_transaction=False)
    processed_4form_df_ri_2 = p4ff_2.get_transactions_adjusted_by_file_names()
    processed_4form_df_ri_day_2 = p4ff_2.get_transactions_by_day()


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


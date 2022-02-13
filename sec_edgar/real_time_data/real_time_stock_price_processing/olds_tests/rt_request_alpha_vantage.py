import os
import random
import time

import numpy as np
import requests
import pandas as pd

from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest

path_default_files = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) \
                     + '/real_time_data/real_time_asset_historical_data'


class RealTimeRequestAlphaVantage(AVHistoricalDataRequest):
    def __init__(self, symbol, api_key, look_up_path=None, update_in_look_up_path=False, verbose=True):
        AVHistoricalDataRequest.__init__(self,
                                         symbol=symbol,
                                         api_key=api_key,
                                         look_up_path=look_up_path,
                                         update_in_look_up_path=update_in_look_up_path,
                                         verbose=verbose)
        self.max_date = pd.to_datetime(self.data_df.index[0])

    def check_and_update_historical_data(self, request_latest_date, delta_days_threshold=2, verbose=True):
        # TODO: review implementations! (this and below)
        #   See if today is include! (if not check if can be done by quoteEndPoint
        #   Test with retrieving a new updated date (with this also validate method on base class!)
        if isinstance(request_latest_date, str):
            request_latest_date = pd.to_datetime(request_latest_date)
        timedelta = self.max_date - request_latest_date

        if delta_days_threshold > timedelta.days:
            self.update_historical_data(verbose=verbose)

    def update_historical_data(self, verbose=True):
        if verbose:
            print("\t Updating historical data from AlphaVantage")
        r = requests.get(self.url)
        self._data_json = r.json()
        self.data_df = pd.DataFrame.from_dict(self._data_json['Time Series (Daily)'], orient='index')
        self.data_df.index = self.data_df.index.astype('datetime64[ns]')
        self.data_df = self.data_df.astype({'1. open': 'float64',
                                            '2. high': 'float64',
                                            '3. low': 'float64',
                                            '4. close': 'float64',
                                            '5. volume': 'int64'})
        sleep_interval = [0.5, 2]
        sleep_time = 60.0 / 5 + random.random() * (sleep_interval[1] - sleep_interval[0]) + sleep_interval[0]
        if verbose:
            print("\t Request complete, going to sleep for about: {} [s]".format(sleep_time))
        time.sleep(sleep_time)

        for i in range(9):
            to_remove = "{}. ".format(i)
            for column in list(self.data_df.columns):
                if to_remove in column:
                    self.data_df = self.data_df.rename(columns={column: column.replace(to_remove, "")})

        self.data_df = self.data_df.sort_index(
            ascending=False)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        if self.update_in_look_up_path and self.file_path is not None:
            try:
                if not os.path.exists(self.look_up_path):
                    os.makedirs(self.look_up_path)

                self.data_df.to_csv(self.file_path, index_label="index")
            except Exception as e:
                print("\t Error Saving Data into file_path: ", self.file_path)
                self.e = e
        self.max_date = pd.to_datetime(self.data_df.index[0])

    def _get_pct_change_ahead_series(self, dates, periods_ahead, column='close', name=None):
        """
        Returns pct_change with shifted ahead (or "futures") series values
        """
        raise ValueError("This class if not intended for ahead comparisons, please use AVHistoricalDataRequest")

    def get_pct_change_ahead_series(self, dates, periods_ahead, column='close', name=None):
        """
        Returns pct_change with shifted ahead (or "futures") series values
        """
        raise ValueError("This class if not intended for ahead comparisons, please use AVHistoricalDataRequest")

    def get_shifted_behind_series(self, dates, periods_behind, column='close', name=None):
        """
        Returns shifted ahead (or "futures") series values
        """
        return AVHistoricalDataRequest.get_shifted_behind_series(self, dates, periods_behind, column, name)

    def get_pct_change_behind_series(self, dates, periods_behind, column='close', name=None):
        """
        Returns pct_change with shifted ahead (or "futures") series values
        """
        return AVHistoricalDataRequest.get_pct_change_behind_series(self, dates, periods_behind, column, name)


if __name__ == '__main__':
    api_key_ = "NIAI6K1QQ0KEXACB"

    trow_historical_data = RealTimeRequestAlphaVantage(symbol="TROW",
                                                       api_key=api_key_,
                                                       look_up_path=path_default_files,
                                                       update_in_look_up_path=True)

    docu_historical_data = RealTimeRequestAlphaVantage(symbol="DOCU",
                                                       api_key=api_key_,
                                                       look_up_path=path_default_files,
                                                       update_in_look_up_path=True)

    docu_historical_data_2 = RealTimeRequestAlphaVantage(symbol="DOCU",
                                                         api_key=api_key_,
                                                         look_up_path=path_default_files,
                                                         update_in_look_up_path=True)
    # docu_historical_data_2.update_historical_data()

    request_latest_date_ = "2022-02-11"
    # TODO: test with today and see if it is included
    # docu_historical_data_2.check_and_update_historical_data(request_latest_date_)

    dates_ = ['2021-12-01', '2021-11-30', '2021-11-29', '2021-11-28', '2021-10-29', '2021-10-28']
    pct_change_behind_series = docu_historical_data_2.get_pct_change_behind_series(dates_, periods_behind=15)
    shifted_behind_series = docu_historical_data_2.get_shifted_behind_series(dates_, periods_behind=15)
    shifted_behind_series_0 = docu_historical_data_2.get_shifted_behind_series(dates_, periods_behind=0)

    # TODO: data is not the same as yahoo finance! (there is a jump!) that might worth to take a look!)
    #       Jump in 2021-11-04
    #       Quizas sea bueno comprobar strategia en data de yahoofinanzas!
    #           COmprobar que resultados buenos no sean puros outlayers!
    periods = 15
    column = 'close'
    adjusted_dates = docu_historical_data_2.interpolate_missing_dates(dates_)
    # shifted_series = self.data_df.loc[adjusted_dates, column].shift(periods)
    data_df = docu_historical_data_2.data_df.copy()
    data_df['dates'] = docu_historical_data_2.data_df.index
    shifted_dates = data_df.shift(-periods).loc[adjusted_dates, 'dates']
    data_df.loc[shifted_dates]
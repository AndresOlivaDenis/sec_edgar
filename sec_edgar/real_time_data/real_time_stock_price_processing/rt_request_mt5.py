import os
import MetaTrader5 as mt5
import random
import time

import numpy as np
import requests
import pandas as pd
from MT5PythonScriptsExperts.connectors.connector_one import ConnectorOne

from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest, NoAvailableDate


class RealTimeRequestMt5(AVHistoricalDataRequest):
    def __init__(self, symbol, mt5_connector, verbose=True):
        self.symbol = symbol
        self.mt5_connector = mt5_connector

    def get_shifted_ahead_series(self, dates, periods_ahead, column='close', name=None):
        """
        https://www.mql5.com/en/docs/integration/python_metatrader5/mt5copyratesrange_py
        """
        periods = abs(periods_ahead)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        self.mt5_connector.initialize()
        shifted_list = []
        for date in dates:
            date_from = pd.to_datetime(date)
            date_to = date_from + pd.Timedelta(days=int(periods*360/250))
            rates_frame = pd.DataFrame(mt5.copy_rates_range(self.symbol,
                                                            mt5.TIMEFRAME_D1,
                                                            date_from,
                                                            date_to))
            # rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            # Rates -1 most crecent, 0-> least recent
            if len(rates_frame) == 0:
                raise NoAvailableDate("", self.symbol, date)

            shifted_list.append(rates_frame.iloc[-1][column])

        shifted_series = pd.Series(shifted_list)
        if name is None:
            shifted_series.name = column + " (-{})".format(periods)
        shifted_series.index = dates
        return shifted_series

    def get_pct_change_ahead_series(self, dates, periods_ahead, column='close', name=None):
        """
        https://www.mql5.com/en/docs/integration/python_metatrader5/mt5copyratesrange_py
        """
        periods = abs(periods_ahead)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        self.mt5_connector.initialize()
        shifted_list = []
        for date in dates:
            date_from = pd.to_datetime(date)
            date_to = date_from + pd.Timedelta(days=int(periods*360/250))
            rates_frame = pd.DataFrame(mt5.copy_rates_range(self.symbol,
                                                            mt5.TIMEFRAME_D1,
                                                            date_from,
                                                            date_to))
            # rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            # Rates -1 most crecent, 0-> least recent
            if len(rates_frame) == 0:
                raise NoAvailableDate("", self.symbol, date)

            shifted_list.append((rates_frame.iloc[-1][column] - rates_frame.iloc[0][column]) / rates_frame.iloc[0][
               column])

        shifted_series = pd.Series(shifted_list)
        if name is None:
            shifted_series.name = column + " (-{})".format(periods)
        shifted_series.index = dates
        return shifted_series

    def get_shifted_behind_series(self, dates, periods_behind, column='close', name=None):
        """
        https://www.mql5.com/en/docs/integration/python_metatrader5/mt5copyratesfrom_py
        """
        periods = abs(periods_behind)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        self.mt5_connector.initialize()
        shifted_list = []
        for date in dates:
            rates_frame = pd.DataFrame(mt5.copy_rates_from(self.symbol,
                                                           mt5.TIMEFRAME_D1,
                                                           pd.to_datetime(date),
                                                           periods + 1))
            # rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            # Rates -1 most crecent, 0-> least recent
            if len(rates_frame) == 0:
                raise NoAvailableDate("", self.symbol, date)

            shifted_list.append(rates_frame.iloc[0][column])

        shifted_series = pd.Series(shifted_list)
        if name is None:
            shifted_series.name = column + " (-{})".format(periods)
        shifted_series.index = dates
        return shifted_series

    def get_pct_change_behind_series(self, dates, periods_behind, column='close', name=None):
        """
        https://www.mql5.com/en/docs/integration/python_metatrader5/mt5copyratesfrom_py
        """
        periods = abs(periods_behind)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        self.mt5_connector.initialize()
        shifted_list = []
        for date in dates:
            rates_frame = pd.DataFrame(mt5.copy_rates_from(self.symbol,
                                                           mt5.TIMEFRAME_D1,
                                                           pd.to_datetime(date),
                                                           periods + 1))
            # rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

            # Rates -1 most crecent, 0-> least recent
            if len(rates_frame) == 0:
                raise NoAvailableDate("", self.symbol, date)

            shifted_list.append((rates_frame.iloc[-1][column] - rates_frame.iloc[0][column]) / rates_frame.iloc[-1][
                column])
        shifted_series = pd.Series(shifted_list)
        if name is None:
            shifted_series.name = column + " (-{})".format(periods)
        shifted_series.index = dates
        return shifted_series


if __name__ == '__main__':
    mt5_connector_ = ConnectorOne()

    docu_historical_data = RealTimeRequestMt5(symbol="DOCU.NAS", mt5_connector=mt5_connector_)

    request_latest_date_ = "2022-02-11"
    # TODO: test with today and see if it is included
    # docu_historical_data_2.check_and_update_historical_data(request_latest_date_)

    dates_ = ['2021-12-01', '2021-11-30', '2021-11-29', '2021-11-28', '2021-10-29', '2021-10-28', '2021-10-28']
    pct_change_behind_series = docu_historical_data.get_pct_change_behind_series(dates_, periods_behind=15)
    shifted_behind_series = docu_historical_data.get_shifted_behind_series(dates_, periods_behind=15)
    shifted_behind_series_0 = docu_historical_data.get_shifted_behind_series(dates_, periods_behind=0)


    mt5_connector_.initialize()
    rates = mt5.copy_rates_from("DOCU.NAS", mt5.TIMEFRAME_D1, pd.to_datetime("2021-12-01"), 15 + 1)
    rates_frame_ = pd.DataFrame(rates)
    # convert time in seconds into the datetime format
    rates_frame_['time'] = pd.to_datetime(rates_frame_['time'], unit='s')
    mt5_connector_.shutdown()

    path_default_files = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) \
                         + '/real_time_data/real_time_asset_historical_data'

    aal_historical_data = AVHistoricalDataRequest(symbol="AAL",
                                                  api_key="NIAI6K1QQ0KEXACB",
                                                  look_up_path=path_default_files,
                                                  update_in_look_up_path=True)


    all_mt5_historical_data = RealTimeRequestMt5(symbol="AAL.NAS", mt5_connector=mt5_connector_)

    df_tests = pd.DataFrame()
    df_tests['AV_aal_pct_change'] = aal_historical_data.get_pct_change_behind_series(dates_, periods_behind=15)
    df_tests['AV_aal_price'] = aal_historical_data.get_shifted_behind_series(dates_, periods_behind=15)
    df_tests['mt5_aal_pct_change'] = all_mt5_historical_data.get_pct_change_behind_series(dates_, periods_behind=15)
    df_tests['mt5_aal_price'] = all_mt5_historical_data.get_shifted_behind_series(dates_, periods_behind=15)

    # TODO: test this by hand and see why!
    df_tests_a = pd.DataFrame()
    df_tests_a['AV_aal_pct_change'] = aal_historical_data.get_pct_change_ahead_series(dates_, periods_ahead=15)
    df_tests_a['AV_aal_price'] = aal_historical_data.get_shifted_ahead_series(dates_, periods_ahead=15)
    df_tests_a['mt5_aal_pct_change'] = all_mt5_historical_data.get_pct_change_ahead_series(dates_, periods_ahead=15)
    df_tests_a['mt5_aal_price'] = all_mt5_historical_data.get_shifted_ahead_series(dates_, periods_ahead=15)


    request_latest_date_ = "2022-02-11"
    mt5_connector_.initialize()
    rates = mt5.copy_rates_from("DOCU.NAS", mt5.TIMEFRAME_D1, pd.to_datetime(request_latest_date_), 15 + 1)
    rates_frame_ = pd.DataFrame(rates)
    # convert time in seconds into the datetime format
    rates_frame_['time'] = pd.to_datetime(rates_frame_['time'], unit='s')

    date_ = "2022-01-21"
    periods_ = 15
    date_from_ = pd.to_datetime(date_)
    date_to_ = date_from_ + pd.Timedelta(days=int(periods_ * 360 / 250))
    rates_from = mt5.copy_rates_range("DOCU.NAS", mt5.TIMEFRAME_D1, date_from_, date_to_)
    rates_from_frame = pd.DataFrame(rates_from)
    # convert time in seconds into the datetime format
    rates_from_frame['time'] = pd.to_datetime(rates_from_frame['time'], unit='s')

    mt5_connector_.shutdown()







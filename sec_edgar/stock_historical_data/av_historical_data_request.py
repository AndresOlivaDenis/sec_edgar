import os
import numpy as np
import requests
import pandas as pd

path_default_files = os.path.dirname(os.path.dirname(os.getcwd())) + '/Data/asset_historical_data'


class AVHistoricalDataRequest(object):

    def __init__(self, symbol, api_key, look_up_path=None, update_in_look_up_path=False, verbose=True):
        self.symbol = symbol
        self.api_key = api_key
        self.look_up_path = look_up_path
        self.update_in_look_up_path = update_in_look_up_path
        self.url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED' \
                   '&symbol={symbol}&outputsize=full&apikey={api_key}'.format(symbol=symbol, api_key=api_key)

        self.file_path = None
        if look_up_path is not None:
            self.file_path = os.path.join(look_up_path, symbol + ".csv")

        self._data_json = None
        self.data_df = None
        if verbose:
            print("\n Loading data for symbol: {}".format(symbol))

        if look_up_path is not None:
            if os.path.isfile(self.file_path):
                self.data_df = pd.read_csv(self.file_path, index_col="index")
            if verbose:
                if self.data_df is None:
                    print("\t No historical data file have been found in look_up_path")
                else:
                    print(
                        "\t Historical data loading from path. MinDate: {}, MaxDate: {}".format(self.data_df.index[-2],
                                                                                                self.data_df.index[0]))

        if self.data_df is None:
            if verbose:
                print("\t Requesting historical data from AlphaVantage")
            r = requests.get(self.url)
            self._data_json = r.json()
            self.data_df = pd.DataFrame.from_dict(self._data_json['Time Series (Daily)'], orient='index')
            self.data_df.index = self.data_df.index.astype('datetime64[ns]')
            self.data_df = self.data_df.astype({'1. open': 'float64',
                                                '2. high': 'float64',
                                                '3. low': 'float64',
                                                '4. close': 'float64',
                                                '5. adjusted close': 'float64',
                                                '6. volume': 'int64',
                                                '7. dividend amount': 'float64',
                                                '8. split coefficient': 'float64'})

        for i in range(9):
            to_remove = "{}. ".format(i)
            for column in list(self.data_df.columns):
                if to_remove in column:
                    self.data_df = self.data_df.rename(columns={column: column.replace(to_remove, "")})

        self.data_df = self.data_df.sort_index(
            ascending=False)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        if update_in_look_up_path and self.file_path is not None:
            try:
                if not os.path.exists(self.look_up_path):
                    os.makedirs(self.look_up_path)

                self.data_df.to_csv(self.file_path, index_label="index")
            except Exception as e:
                print("\t Error Saving Data into file_path: ", self.file_path)
                self.e = e

    def get_data_df(self):
        return self.data_df.copy()

    def interpolate_missing_dates(self, dates):
        if not isinstance(dates, pd.Series):
            adjusted_dates = pd.Series(dates)
        else:
            adjusted_dates = dates.copy()

        available_dates = self.data_df.index
        isin_dates = np.isin(adjusted_dates, available_dates)
        if isin_dates.all():
            return adjusted_dates

        available_dates_sorted = available_dates.sort_values(ascending=False)
        missing_dates = adjusted_dates[~isin_dates]
        adjusted_missing_dates = []
        for missing_date in missing_dates:
            prev_dates = available_dates_sorted < missing_date
            adjusted_missing_dates.append(available_dates_sorted[prev_dates][0])

        adjusted_dates.loc[~isin_dates] = adjusted_missing_dates
        return adjusted_dates

    def get_shifted_ahead_series(self, dates, periods_ahead, column='adjusted close', name=None):
        """
        Returns shifted ahead (or "futures") series values
        """
        periods = abs(periods_ahead)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        adjusted_dates = self.interpolate_missing_dates(dates)
        # shifted_series = self.data_df.loc[adjusted_dates, column].shift(periods)
        shifted_series = self.data_df.shift(periods).loc[adjusted_dates, column]

        if name is None:
            shifted_series.name = column + " (+{})".format(periods)
        shifted_series.index = dates
        return shifted_series

    def _get_pct_change_ahead_series(self, dates, periods_ahead, column='adjusted close', name=None):
        """
        Returns pct_change with shifted ahead (or "futures") series values
        """
        periods = abs(periods_ahead)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        adjusted_dates = self.interpolate_missing_dates(dates)
        shifted_series = -self.data_df.pct_change(periods).loc[adjusted_dates, column]
        # shifted_series = -self.data_df.pct_change(periods).loc[adjusted_dates, column]
        # shifted_series = -np.log(self.data_df).diff(periods).loc[adjusted_dates, column]
        # shifted_series = -np.log(self.data_df[column]).diff(periods).loc[adjusted_dates]

        if name is None:
            shifted_series.name = column + " pct_change (+{})".format(periods)
        shifted_series.index = dates
        return shifted_series

    def get_pct_change_ahead_series(self, dates, periods_ahead, column='adjusted close', name=None):
        """
        Returns pct_change with shifted ahead (or "futures") series values
        """
        periods = abs(periods_ahead)  # To avoid confusion!! Convention 0 -> most recent, -1 -> last

        adjusted_dates = self.interpolate_missing_dates(dates)
        shifted_series = -np.log(self.data_df[[column]]).diff(periods).loc[adjusted_dates, column]
        shifted_series = np.exp(shifted_series) - 1.0

        if name is None:
            shifted_series.name = column + " pct_change (+{})".format(periods)
        shifted_series.index = dates
        return shifted_series


class MultiSymbolAVHistoricalDataRequest(object):

    def __init__(self, symbol_list, api_key, look_up_path=None, update_in_look_up_path=False, verbose=True):
        # TODO: same as AVHistoricalDataRequest but with a list of symbols ¿?
        pass


if __name__ == '__main__':
    api_key_ = "NIAI6K1QQ0KEXACB"

    intc_historical_data = AVHistoricalDataRequest(symbol="INTC",
                                                   api_key=api_key_,
                                                   look_up_path=path_default_files,
                                                   update_in_look_up_path=True)

    amd_historical_data = AVHistoricalDataRequest(symbol="AMD",
                                                  api_key=api_key_,
                                                  look_up_path=path_default_files,
                                                  update_in_look_up_path=True)

    data_df_ = amd_historical_data.data_df.copy()

    dates_ = ['2021-12-01', '2021-11-30', '2021-11-29', '2021-11-28']
    shifted_series_ = amd_historical_data.get_shifted_ahead_series(dates=dates_, periods_ahead=1)
    tests_df = amd_historical_data.get_shifted_ahead_series(dates=amd_historical_data.data_df.index, periods_ahead=1)
    tests_df_ = amd_historical_data.get_shifted_ahead_series(dates=amd_historical_data.data_df.index[::-1],
                                                             periods_ahead=1)

    pct_change_ahead_series_ = amd_historical_data.get_pct_change_ahead_series(dates=dates_, periods_ahead=1)
    dates_.reverse()
    pct_change_ahead_series_rev = amd_historical_data.get_pct_change_ahead_series(dates=dates_, periods_ahead=1)
    pct_change_ahead_series_a = amd_historical_data.get_pct_change_ahead_series(dates=amd_historical_data.data_df.index,
                                                                                periods_ahead=1)

    intc_pct_change_ahead_1 = intc_historical_data.get_pct_change_ahead_series(dates=intc_historical_data.data_df.index,
                                                                               periods_ahead=1)
    intc_pct_change_ahead_2 = intc_historical_data.get_pct_change_ahead_series(dates=intc_historical_data.data_df.index,
                                                                               periods_ahead=2)
    intc_pct_change_ahead_3 = intc_historical_data.get_pct_change_ahead_series(dates=intc_historical_data.data_df.index,
                                                                               periods_ahead=3)

    # TODO: compare vs "by hand computations"   # And Add unittests ¿?

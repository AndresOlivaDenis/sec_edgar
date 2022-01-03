import pandas as pd
from scipy import stats
from scipy.stats.distributions import t
from scipy.stats.distributions import beta


def compute_performance_means_series(performance_cik_df):
    performance_means_series = pd.Series(performance_cik_df.mean(),
                                         dtype='O')

    performance_means_series['is_pos_tse_ok'] = performance_means_series['Positive_tsa'] > performance_means_series[
        'benchmark']
    performance_means_series['is_greater_ok'] = performance_means_series['is_greater'] > performance_means_series[
        'is_less']

    return performance_means_series


class PerformanceMetric01Ints(object):

    def __init__(self, column_label_eval='Price pct_change (5)'):
        self.column_label_eval = column_label_eval
        self.performance_means_series = None
        self.performance_cik_df = None

    def eval(self, processed_form4_df):
        """
        Evaluate processed_form4_df performance.
        Positive transactionSharesAdjust are evaluated under differents labels_eval.
        All transactions are considered as benchmark.
        Means are computed first by CIK grouping and them overall mean.

        Returns:
            performance_cik_df_dict -> dict of pd.DataFrame performance per label eval list
            labels_performance_df -> overall performance per label eval list
        """
        processed_form4_df['to_eval_level'] = (processed_form4_df[self.column_label_eval] > 0.0).astype(int)
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]


        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')['to_eval_level'].mean()
        performance_cik_df['benchmark'] = processed_form4_df.groupby('CIK')['to_eval_level'].mean()
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']
        performance_cik_df['is_less'] = performance_cik_df['Positive_tsa'] < performance_cik_df['benchmark']
        performance_cik_df['is_equal'] = performance_cik_df['Positive_tsa'] == performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        return performance_means_series


class PerformanceMetric02Ints(object):

    def __init__(self,
                 column_label_eval='Price pct_change (5)',
                 shifted_columns_label_eval='Shifted Price pct_change (5)(-5 days 00:00:00)'):
        self.column_label_eval = column_label_eval
        self.shifted_columns_label_eval = shifted_columns_label_eval
        self.performance_means_series = None
        self.performance_cik_df = None

    def eval(self, processed_form4_df):
        """
        Evaluate processed_form4_df performance.
        Positive transactionSharesAdjust are evaluated under differents labels_eval.
        Returns on given +-deltaTime are considered as benchmark.
        Means are computed first by CIK grouping and them overall mean.

        Returns:
            performance_cik_df_dict -> dict of pd.DataFrame performance per label eval list
            labels_performance_df -> overall performance per label eval list
        """
        processed_form4_df['to_eval_level'] = (processed_form4_df[self.column_label_eval] > 0.0).astype(int)
        processed_form4_df['to_eval_level_shifted'] = (processed_form4_df[self.shifted_columns_label_eval] > 0.0).astype(int)
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')['to_eval_level'].mean()
        performance_cik_df['benchmark'] = processed_form4_df_pos.groupby('CIK')['to_eval_level_shifted'].mean()
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']
        performance_cik_df['is_less'] = performance_cik_df['Positive_tsa'] < performance_cik_df['benchmark']
        performance_cik_df['is_equal'] = performance_cik_df['Positive_tsa'] == performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        return performance_means_series


class PerformanceMetric03Ints(object):

    def __init__(self,
                 column_label_eval='Price pct_change (5)',
                 shifted_columns_label_eval='Shifted Price pct_change (5)(-5 days 00:00:00)'):
        self.column_label_eval = column_label_eval
        self.shifted_columns_label_eval = shifted_columns_label_eval
        self.performance_means_series = None
        self.performance_cik_df = None

    def eval(self, processed_form4_df):
        """
        Evaluate processed_form4_df performance.
        Positive transactionSharesAdjust are evaluated under differents labels_eval.
        Returns on given +-deltaTime are considered as benchmark.
        Means are computed first by CIK grouping and them overall mean.

        Returns:
            performance_cik_df_dict -> dict of pd.DataFrame performance per label eval list
            labels_performance_df -> overall performance per label eval list
        """
        processed_form4_df['to_eval_level'] = (processed_form4_df[self.column_label_eval] > 0.0).astype(int)
        processed_form4_df['to_eval_level_shifted'] = (processed_form4_df[self.shifted_columns_label_eval] > 0.0).astype(int)
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos['to_eval_level']
        performance_cik_df['benchmark'] = processed_form4_df_pos['to_eval_level_shifted']
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']
        performance_cik_df['is_less'] = performance_cik_df['Positive_tsa'] < performance_cik_df['benchmark']
        performance_cik_df['is_equal'] = performance_cik_df['Positive_tsa'] == performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        return performance_means_series


# TODO: define a new metric with this ? (as metric 2 with addition of threes_hold)
#   Is really a metric ? is not more something about processing ?
#   Study shares_percent_changes_mean per interval ranges values!
#   Review 4form and processing! (and how shares_percent_changes_mean is computed!)
#       Add timeDelta of declared date - transaction date (by all!) (this seems nice!!)
#   Study inclusion or not of derivative transactions

# threes_hold = 1 / 100
# processed_4form_df_ri_day = processed_4form_df_ri_day[processed_4form_df_ri_day['shares_percent_changes_mean'] > threes_hold]

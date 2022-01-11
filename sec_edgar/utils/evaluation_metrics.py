import pandas as pd
from scipy import stats
from scipy.stats.distributions import t


def compute_performance_means_series(performance_cik_df):
    performance_means_series = pd.Series(performance_cik_df.mean().rename({'is_greater': 'is_greater_prob'}),
                                         dtype='O')

    performance_means_series['is_pos_tse_ok'] = performance_means_series['Positive_tsa'] > performance_means_series[
        'benchmark']
    performance_means_series['is_greater_prob_ok'] = performance_means_series['is_greater_prob'] > 0.5
    performance_means_series['is_performance_ok'] = performance_means_series['is_pos_tse_ok'] and \
                                                    performance_means_series['is_greater_prob_ok']

    performance_means_series_stds = performance_cik_df.std()
    performance_means_series['Positive_tsa_std'] = performance_means_series_stds['Positive_tsa']
    performance_means_series['benchmark_std'] = performance_means_series_stds['benchmark']

    # https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test
    A = performance_cik_df['Positive_tsa']
    B = performance_cik_df['benchmark']
    n1, n2 = len(A), len(B)
    s1, s2 = A.std(), B.std()

    sp = (((n1 - 1) * s1 ** 2 + (n2 - 1) * s2 ** 2) / (n1 + n2 - 2)) ** 0.5
    t_statistic = (A.mean() - B.mean()) / (sp * (1 / n1 + 1 / n2) ** 0.5)
    df = (s1 ** 2 / n1 + s2 ** 2 / n2) ** 2 / ((s1 ** 2 / n1) ** 2 / (n1 - 1) + (s2 ** 2 / n2) ** 2 / (n2 - 1))

    performance_means_series['statistic'] = t_statistic
    performance_means_series['pvalue'] = 1 - t(df=df).cdf(t_statistic)

    return performance_means_series


class PerformanceMetric01(object):

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
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')[self.column_label_eval].mean()
        performance_cik_df['benchmark'] = processed_form4_df.groupby('CIK')[self.column_label_eval].mean()
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        performance_means_series['len'] = len(processed_form4_df_pos)

        return performance_means_series


class PerformanceMetric02(object):

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
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos.groupby('CIK')[self.column_label_eval].mean()
        performance_cik_df['benchmark'] = processed_form4_df_pos.groupby('CIK')[self.shifted_columns_label_eval].mean()
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()
        performance_means_series['len'] = len(processed_form4_df_pos)

        return performance_means_series


class PerformanceMetric03(object):

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
        processed_form4_df_pos = processed_form4_df[processed_form4_df['transactionSharesAdjust'] > 0]

        performance_cik_df = pd.DataFrame()
        performance_cik_df['Positive_tsa'] = processed_form4_df_pos[self.column_label_eval]
        performance_cik_df['benchmark'] = processed_form4_df_pos[self.shifted_columns_label_eval]
        performance_cik_df['is_greater'] = performance_cik_df['Positive_tsa'] > performance_cik_df['benchmark']

        performance_means_series = compute_performance_means_series(performance_cik_df)

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        performance_means_series['len'] = len(processed_form4_df_pos)

        return performance_means_series


# TODO: define a new metric with this ? (as metric 2 with addition of threes_hold)
#   Is really a metric ? is not more something about processing ?
#   Study shares_percent_changes_mean per interval ranges values!
#   Review 4form and processing! (and how shares_percent_changes_mean is computed!)
#       Add timeDelta of declared date - transaction date (by all!) (this seems nice!!)
#   Study inclusion or not of derivative transactions

# threes_hold = 1 / 100
# processed_4form_df_ri_day = processed_4form_df_ri_day[processed_4form_df_ri_day['shares_percent_changes_mean'] > threes_hold]

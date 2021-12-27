import pandas as pd
from scipy import stats


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

        A = performance_means_series['Positive_tsa']
        B = performance_means_series['benchmark']
        t_test = stats.ttest_ind(A, B, equal_var=False, alternative='greater')
        performance_means_series['statistic'] = t_test.statistic
        performance_means_series['pvalue'] = t_test.pvalue

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

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

        A = performance_means_series['Positive_tsa']
        B = performance_means_series['benchmark']
        t_test = stats.ttest_ind(A, B, equal_var=False, alternative='greater')
        performance_means_series['statistic'] = t_test.statistic
        performance_means_series['pvalue'] = t_test.pvalue

        self.performance_means_series = performance_means_series.copy()
        self.performance_cik_df = performance_cik_df.copy()

        return performance_means_series

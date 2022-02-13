# Split 4form_df by years
# Split 4form_df by cik grouping

# def eval_model_estimates(model_4form, column_to_eval) .....
#       compute stats ?

# def compare_4form_results(model1_4form, bench_4form) .....
#       compare stats:  P(x1) > P(x_bench)

import pandas as pd
from scipy.stats.distributions import t

from sec_edgar.new_performance_evals.dataset_splits import DataSets4FormSplits

from sec_edgar.new_performance_evals.datasets_adjustments import DataSetsAdjustments


class Models4FormPerformanceEval(object):

    @staticmethod
    def model_4form_df_dict_to_series(model_4form_df_dict, column):
        # TODO: test!!
        model_4form_series_dict = {ds_name: model_4form_df[column]
                                   for ds_name, model_4form_df in model_4form_df_dict.items()}
        return model_4form_series_dict

    @staticmethod
    def eval_multiple_ds(model1_4form_series_dict, bench_4form_series_dict):
        # TODO: test!!
        results_df = pd.DataFrame()
        for ds_name in model1_4form_series_dict.keys():
            results_df[ds_name] = Models4FormPerformanceEval.compare_4form_results(model1_4form_series_dict[ds_name],
                                                                                   bench_4form_series_dict[ds_name],
                                                                                   out_name=ds_name)
        return results_df

    @staticmethod
    def compare_4form_results(model1_4form_series, bench_4form_series, out_name=None):
        name_1 = model1_4form_series.name
        name_2 = bench_4form_series.name

        result_dict = {
            "{}_mean".format(name_1): model1_4form_series.mean(),
            "{}_mean".format(name_2): bench_4form_series.mean(),
            "{}_std".format(name_1): model1_4form_series.std(),
            "{}_std".format(name_2): bench_4form_series.std(),
            "{}_len".format(name_1): len(model1_4form_series),
            "{}_len".format(name_2): len(bench_4form_series),
        }

        t_statistic, pvalue = Models4FormPerformanceEval.eval_two_sample_t_test(x1=model1_4form_series,
                                                                                x2=bench_4form_series)
        result_dict = {**result_dict,
                       "t_statistic": t_statistic,
                       "pvalue": pvalue,
                       'is_greater': model1_4form_series.mean() > bench_4form_series.mean(),
                       'is_greater (95%)': (model1_4form_series.mean() > bench_4form_series.mean()) and (pvalue < 0.05),
                       }

        return pd.Series(result_dict, name=out_name)

    @staticmethod
    def eval_two_sample_t_test(x1, x2):
        """
        # https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test
        """
        n1, n2 = len(x1), len(x2)
        s1, s2 = x1.std(), x2.std()
        x1_m, x2_m = x1.mean(), x2.mean()

        sp = (((n1 - 1) * s1 ** 2 + (n2 - 1) * s2 ** 2) / (n1 + n2 - 2)) ** 0.5
        t_statistic = (x1_m - x2_m) / (sp * (1 / n1 + 1 / n2) ** 0.5)
        df = (s1 ** 2 / n1 + s2 ** 2 / n2) ** 2 / ((s1 ** 2 / n1) ** 2 / (n1 - 1) + (s2 ** 2 / n2) ** 2 / (n2 - 1))

        t_statistic
        pvalue = 1 - t(df=df).cdf(t_statistic)
        return t_statistic, pvalue


if __name__ == '__main__':
    import os

    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets_u1/'
    file_name = path_processed_datasets + "us_market_allderv_1021.py.csv"

    # path_processed_datasets = base_path + '/Data/processed_datasets/'
    # file_name = path_processed_datasets + "us_market_allderv_shiftedDays_1019.py.csv"

    processed_4form_df = pd.read_csv(file_name, index_col=0)
    processed_form4_df_pos = processed_4form_df[processed_4form_df['transactionSharesAdjust'] > 0]

    # ----------------------------------------------------------------------------------------------------------------

    # Performance Metrics Evaluations --------------------------------------------------------------------------------
    print("\n processed_form4_df_pos['Price pct_change (15)'] vs processed_form4_df_pos['GSPC Price pct_change (15)']")

    m1 = processed_form4_df_pos['Price pct_change (15)']
    bench = processed_form4_df_pos['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)

    # ----------------------------------------------------------------------------------------------------------------
    print("\n processed_form4_df_pos['Price pct_change (15)'] vs processed_4form_df['GSPC Price pct_change (15)']")

    m1 = processed_form4_df_pos['Price pct_change (15)']
    bench = processed_4form_df['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)

    # ----------------------------------------------------------------------------------------------------------------
    print("\n processed_form4_df_pos['GSPC Price pct_change (15)'] vs processed_4form_df['GSPC Price pct_change (15)']")

    m1 = processed_form4_df_pos['GSPC Price pct_change (15)']
    m1.name = 'GSPC Price pct_change (15) (pos)'
    bench = processed_4form_df['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)

    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    ds4fs = DataSets4FormSplits(processed_4form_df)
    # ----------------------------------------------------------------------------------------------------------------

    # split_by_cik_grouping ------------------------------------------------------------------------------------------
    form4_df_cik_grouping_dict_ = ds4fs.split_by_cik_grouping(n_sub_set=4)
    form4_df_cik_grouping_dict_pos = {ds_name: form4_df_cik[form4_df_cik['transactionSharesAdjust'] > 0]
                                      for ds_name, form4_df_cik in form4_df_cik_grouping_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_pos,
                                                                          'Price pct_change (15)')

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_,
                                                                             'GSPC Price pct_change (15)')

    print("\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_cik_grouping_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # split by years -------------------------------------------------------------------------------------------------
    form4_df_years_dict_ = ds4fs.split_by_years()

    form4_df_years_dict_pos = {ds_name: form4_df_cik[form4_df_cik['transactionSharesAdjust'] > 0]
                                      for ds_name, form4_df_cik in form4_df_years_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_pos,
                                                                          'Price pct_change (15)')

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_pos, #form4_df_years_dict_,
                                                                             'GSPC Price pct_change (15)')

    print("\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_years_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    print("\n processed_form4_df['Price pct_change (15)'] vs processed_form4_df['GSPC Price pct_change (15)']")

    processed_4form_df_adjusted = DataSetsAdjustments.limit_n_max_per_cik(processed_4form_df, n_max_per_cik=None)
    processed_form4_df_pos_adjusted = processed_4form_df_adjusted[processed_4form_df_adjusted['transactionSharesAdjust'] > 0]

    # m1 = processed_4form_df_adjusted['Price pct_change (15)']
    m1 = processed_form4_df_pos_adjusted['Price pct_change (15)']

    bench = processed_4form_df_adjusted['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)

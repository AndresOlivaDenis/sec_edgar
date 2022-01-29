import os
import numpy as np
import matplotlib.pyplot as plt
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest
import pandas as pd

from sec_edgar.utils.evaluation_metrics import PerformanceMetric01, PerformanceMetric02, PerformanceMetric03
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form, PerformanceEvaluations4FormYear

pd.set_option('display.width', 1500)      # max table width to display

if __name__ == '__main__':
    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_4form_df = pd.read_csv(file_name, index_col=0)

    # ----------------------------------------------------------------------------------------------------------------

    # Performance Metrics Evaluations --------------------------------------------------------------------------------
    print("=" * 90)
    pe_4form = PerformanceEvaluations4Form(processed_4form_df, n_sub_set=3)

    # PerformanceMetric01 evaluation:
    metrics_df_01, _ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric01,
                                            pm_kwargs=dict(column_label_eval='Price pct_change (10)'))
    print("\nPerformanceMetric01 evaluation: ")
    print(metrics_df_01)
    print("-" * 75)

    # PerformanceMetric02 evaluation:
    metrics_df_02, _ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric02,
                                            pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                           shifted_columns_label_eval=
                                                           'Shifted Price pct_change (10)(-10 days +00:00:00)'))

    metrics_df_02b, _ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric02,
                                             pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                            shifted_columns_label_eval=
                                                            'Shifted Price pct_change (10)(10 days 00:00:00)'))
    print("\nPerformanceMetric02 evaluation: ")
    print(metrics_df_02)
    print("-" * 75)

    # PerformanceMetric03 evaluation:
    metrics_df_03, _ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric03,
                                            pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                           shifted_columns_label_eval=
                                                           'Shifted Price pct_change (10)(-10 days +00:00:00)'))

    metrics_df_03b, _ = pe_4form.eval_metric(performance_metric_ref=PerformanceMetric03,
                                             pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                            shifted_columns_label_eval=
                                                            'Shifted Price pct_change (10)(10 days 00:00:00)'))

    print("\nPerformanceMetric03 evaluation: ")
    print(metrics_df_03)
    print("-" * 75)

    print("=" * 90)
    # Performance Metrics Evaluations Year ----------------------------------------------------------------------------
    print("=" * 90)
    pe_4form_year = PerformanceEvaluations4FormYear(processed_4form_df)

    # PerformanceMetric01 evaluation:
    metrics_df_01_year, _ = pe_4form_year.eval_metric(performance_metric_ref=PerformanceMetric01,
                                                      pm_kwargs=dict(column_label_eval='Price pct_change (10)'))
    print("\nPerformanceMetric01 evaluation (Year): ")
    print(metrics_df_01_year)
    print("-" * 75)

    # PerformanceMetric02 evaluation:
    metrics_df_02_year, _ = pe_4form_year.eval_metric(performance_metric_ref=PerformanceMetric02,
                                                      pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                                     shifted_columns_label_eval=
                                                                     'Shifted Price pct_change (10)(-10 days +00:00:00)'))

    metrics_df_02b_year, _ = pe_4form_year.eval_metric(performance_metric_ref=PerformanceMetric02,
                                                       pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                                      shifted_columns_label_eval=
                                                                      'Shifted Price pct_change (10)(10 days 00:00:00)'))
    print("\nPerformanceMetric02 evaluation (Year): ")
    print(metrics_df_02_year)
    print("-" * 75)

    # PerformanceMetric03 evaluation:
    metrics_df_03_year, _ = pe_4form_year.eval_metric(performance_metric_ref=PerformanceMetric03,
                                                      pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                                     shifted_columns_label_eval=
                                                                     'Shifted Price pct_change (10)(-10 days +00:00:00)'))

    metrics_df_03b_year, _ = pe_4form_year.eval_metric(performance_metric_ref=PerformanceMetric03,
                                                       pm_kwargs=dict(column_label_eval='Price pct_change (10)',
                                                                      shifted_columns_label_eval=
                                                                      'Shifted Price pct_change (10)(10 days 00:00:00)'))

    print("\nPerformanceMetric03 evaluation (Year): ")
    print(metrics_df_03_year)
    print("-" * 75)

    print("=" * 90)

    # TODO:
    #  evaluate: my_derivative_types        (1)

    # TODO:
    #   Compare vs market
    processed_4form_df['transaction_value'] = processed_4form_df['Shifted Price (0)'] * processed_4form_df[
        'transactionSharesAdjust']

    group_by = processed_4form_df.groupby(['Date_Filed'])
    # Select columns
    processed_4form_df_market = pd.DataFrame()
    columns_to_mean = ['Shifted Price pct_change (5)(5 days 00:00:00)', 'transaction_value']
    for column in columns_to_mean:
        processed_4form_df_market[column] = group_by[column].mean()
    print("all: ", processed_4form_df_market['Shifted Price pct_change (5)(5 days 00:00:00)'].mean())
    positive = processed_4form_df_market[processed_4form_df_market['transaction_value'] > 0.0]
    print("positive: ", positive['Shifted Price pct_change (5)(5 days 00:00:00)'].mean())
    # TODO: actually plot!

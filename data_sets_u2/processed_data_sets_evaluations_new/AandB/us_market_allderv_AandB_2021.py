import os
import numpy as np
import matplotlib.pyplot as plt
from sec_edgar.data.postprocessing_pre_evals.postprocess_AandB_pos_model import PostProcess4FormPosAandB
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.new_performance_evals.dataset_splits import DataSets4FormSplits
from sec_edgar.new_performance_evals.datasets_adjustments import DataSetsAdjustments
from sec_edgar.new_performance_evals.models_performance_evaluations import Models4FormPerformanceEval
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest
import pandas as pd

from sec_edgar.utils.evaluation_metrics import PerformanceMetric01, PerformanceMetric02, PerformanceMetric03
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form, PerformanceEvaluations4FormYear

pd.set_option('display.width', 1500)  # max table width to display

if __name__ == '__main__':
    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))

    path_processed_datasets = base_path + '/Data/processed_datasets_u2/'
    file_name = path_processed_datasets + "us_market_allderv_2021.py.csv"
    processed_4form_df = pd.read_csv(file_name, index_col=0)

    # ----------------------------------------------------------------------------------------------------------------
    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    ds4fs = DataSets4FormSplits(processed_4form_df)
    # ----------------------------------------------------------------------------------------------------------------

    column_label_eval = 'Price pct_change (15)'
    bench_column_label_eval = 'GSPC Price pct_change (15)'

    # split_by_cik_grouping ------------------------------------------------------------------------------------------
    form4_df_cik_grouping_dict_ = ds4fs.split_by_cik_grouping(n_sub_set=4)
    form4_df_cik_grouping_dict_post_proc = {ds_name: PostProcess4FormPosAandB.post_process_df(form4_df_cik)
                                            for ds_name, form4_df_cik in form4_df_cik_grouping_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_post_proc,
                                                                          column_label_eval)

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_post_proc,
                                                                             bench_column_label_eval)

    print("\n {} (form4_df_cik_grouping_dict_pos) vs {} (form4_df_cik_grouping_dict_pos)".format(column_label_eval,
                                                                                              bench_column_label_eval))

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # split by years -------------------------------------------------------------------------------------------------
    form4_df_years_dict_ = ds4fs.split_by_years()
    bench_column_label_eval = 'Price pct_change (-15)'

    form4_df_years_dict_post_proc = {ds_name: PostProcess4FormPosAandB.post_process_df(form4_df_year)
                                     for ds_name, form4_df_year in form4_df_years_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_post_proc,
                                                                          column_label_eval)

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_post_proc,
                                                                             # form4_df_years_dict_,
                                                                             bench_column_label_eval)

    print("\n {} (form4_df_cik_grouping_dict_pos) vs {} (form4_df_years_dict_pos)".format(column_label_eval,
                                                                                          bench_column_label_eval))
    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

import os
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.new_performance_evals.dataset_splits import DataSets4FormSplits
from sec_edgar.new_performance_evals.datasets_adjustments import DataSetsAdjustments
from sec_edgar.new_performance_evals.models_performance_evaluations import Models4FormPerformanceEval
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest

from sec_edgar.stock_historical_data.av_historical_data_request import NoAvailableDate

path_yf_indices_historical_data = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))) + '/Data/yf_indices_historical_data'

bench_reference_symbol = "GSPC"
pd.set_option('max_columns', None)


class PostProcessPosIsDirector(object):
    """
    Processed columns needed:
        - my_derivative_types
        - transactionSharesAdjust
    """

    @staticmethod
    def post_process_df(form4_df,
                        ):
        form4_df_post_proc = form4_df.copy()
        # form4_df_post_proc = form4_df_post_proc[form4_df_post_proc.my_derivative_types.isin(['AB', 'B'])]
        form4_df_post_proc = form4_df_post_proc[form4_df_post_proc.my_derivative_types.isin(['A'])]
        form4_df_post_proc = form4_df_post_proc[~processed_4form_df.isDirector.str.contains("1")]
        return form4_df_post_proc


if __name__ == '__main__':
    import os

    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))

    path_processed_datasets = base_path + '/Data/processed_datasets_u2/'
    file_name = path_processed_datasets + "us_market_allderv_1021.py.csv"

    # path_processed_datasets = base_path + '/Data/processed_datasets/'
    # file_name = path_processed_datasets + "us_market_allderv_shiftedDays_1019.py.csv"

    processed_4form_df = pd.read_csv(file_name, index_col=0)

    # ----------------------------------------------------------------------------------------------------------------

    # ModelDF --------------------------------------------------------------------------------------------

    processed_form4_df_model = PostProcessPosIsDirector.post_process_df(processed_4form_df)

    print("\n processed_form4_df_model vs processed_form4_df_pos['GSPC Price pct_change (15)']")

    m1 = processed_form4_df_model['Price pct_change (15)']
    bench = processed_form4_df_model['GSPC Price pct_change (15)']
    # bench = processed_4form_df['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)


    # ----------------------------------------------------------------------------------------------------------------

    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    ds4fs = DataSets4FormSplits(processed_4form_df)
    # ----------------------------------------------------------------------------------------------------------------

    # split_by_cik_grouping ------------------------------------------------------------------------------------------
    form4_df_cik_grouping_dict_ = ds4fs.split_by_cik_grouping(n_sub_set=4)

    form4_df_cik_grouping_dict_post_proc = {ds_name: PostProcessPosIsDirector.post_process_df(form4_df_cik)
                                            for ds_name, form4_df_cik in form4_df_cik_grouping_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_post_proc,
                                                                          'Price pct_change (15)')

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_,
                                                                            'GSPC Price pct_change (15)')

    print(
        "\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_cik_grouping_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # split by years -------------------------------------------------------------------------------------------------
    form4_df_years_dict_ = ds4fs.split_by_years()

    form4_df_years_dict_post_proc = {ds_name: PostProcessPosIsDirector.post_process_df(form4_df_year)
                                     for ds_name, form4_df_year in form4_df_years_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_post_proc,
                                                                          'Price pct_change (15)')

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_,
                                                                            'GSPC Price pct_change (15)')


    print(
        "\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_years_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # TODO: see results in this
    processed_4form_df_adjusted = DataSetsAdjustments.limit_n_max_per_cik(processed_4form_df, n_max_per_cik=None)

    # TODO: review is ok, seems to good to be true

    # TODO: build ds without AandB us_market_allderv_AandB_1021

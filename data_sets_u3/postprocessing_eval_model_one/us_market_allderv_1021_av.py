import pandas as pd
from sec_edgar.new_performance_evals.dataset_splits import DataSets4FormSplits
from sec_edgar.new_performance_evals.datasets_adjustments import DataSetsAdjustments
from sec_edgar.new_performance_evals.models_performance_evaluations import Models4FormPerformanceEval

from sec_edgar.data.postprocessing.postprocess_model_one import PostProcessPosChangeLast

bench_reference_symbol = "GSPC"
pd.set_option('max_columns', None)

if __name__ == '__main__':
    import os

    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets_u2/'
    file_name = path_processed_datasets + "us_market_allderv_1021.py.csv"

    # path_processed_datasets = base_path + '/Data/processed_datasets_u3/'
    # file_name = path_processed_datasets + "us_market_allderv_1021_av.py.csv"

    # path_processed_datasets = base_path + '/Data/processed_datasets/'
    # file_name = path_processed_datasets + "us_market_allderv_shiftedDays_1019.py.csv"

    processed_4form_df = pd.read_csv(file_name, index_col=0)

    # ----------------------------------------------------------------------------------------------------------------

    # ModelDF --------------------------------------------------------------------------------------------
    model_column = 'Price pct_change (15)'
    benc_column = 'GSPC Price pct_change (15)'
    upper_interval = -0.075
    processed_form4_df_model = PostProcessPosChangeLast.post_process_df(processed_4form_df, upper_interval=upper_interval)

    print("\n processed_form4_df_model vs processed_form4_df_pos['GSPC Price pct_change (15)']")

    m1 = processed_form4_df_model[model_column]
    bench = processed_form4_df_model[benc_column]
    # bench = processed_4form_df['GSPC Price pct_change (15)']

    performance_eval = Models4FormPerformanceEval.compare_4form_results(m1, bench, out_name="all_DS")
    print(performance_eval)


    # ----------------------------------------------------------------------------------------------------------------

    # DataSets4FormSplits --------------------------------------------------------------------------------------------
    ds4fs = DataSets4FormSplits(processed_4form_df)
    # ----------------------------------------------------------------------------------------------------------------

    # split_by_cik_grouping ------------------------------------------------------------------------------------------
    form4_df_cik_grouping_dict_ = ds4fs.split_by_cik_grouping(n_sub_set=4)

    form4_df_cik_grouping_dict_post_proc = {ds_name: PostProcessPosChangeLast.post_process_df(form4_df_cik, upper_interval=upper_interval)
                                            for ds_name, form4_df_cik in form4_df_cik_grouping_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_post_proc,
                                                                          model_column)

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_cik_grouping_dict_,
                                                                            benc_column)

    print(
        "\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_cik_grouping_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # split by years -------------------------------------------------------------------------------------------------
    form4_df_years_dict_ = ds4fs.split_by_years()

    form4_df_years_dict_post_proc = {ds_name: PostProcessPosChangeLast.post_process_df(form4_df_year, upper_interval=upper_interval)
                                     for ds_name, form4_df_year in form4_df_years_dict_.items()}

    m1_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_post_proc,
                                                                          model_column)

    bench_ds_dict = Models4FormPerformanceEval.model_4form_df_dict_to_series(form4_df_years_dict_,
                                                                            benc_column)


    print(
        "\n form4_df_cik_grouping_dict_pos['Price pct_change (15)'] vs form4_df_years_dict_['GSPC Price pct_change (15)']")

    performance_eval_ds_df = Models4FormPerformanceEval.eval_multiple_ds(m1_ds_dict, bench_ds_dict)
    print(performance_eval_ds_df)

    # ----------------------------------------------------------------------------------------------------------------

    # TODO: see results in this
    processed_4form_df_adjusted = DataSetsAdjustments.limit_n_max_per_cik(processed_4form_df, n_max_per_cik=None)

    dates_index = pd.DatetimeIndex(processed_form4_df_model.Date_Filed)
    year_months_dates = dates_index.year.astype(str) + "/" + dates_index.month.astype(str)
    processed_form4_df_model['year_months_dates'] = year_months_dates
    year_mont_counts_series = processed_form4_df_model.groupby('year_months_dates').CIK.count()
    # TODO: review is ok, seems to good to be true

    # TODO: build ds without AandB us_market_allderv_AandB_1021

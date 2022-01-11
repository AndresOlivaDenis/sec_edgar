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

from sec_edgar.utils.evaluation_metrics import PerformanceMetric02
from sec_edgar.utils.performance_evaluations import PerformanceEvaluations4Form

if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_company_ticket_file = base_path + "/Data/company_tickers.csv"
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + os.path.basename(__file__).replace("_extended", "") + ".csv"
    processed_4form_df = pd.read_csv(file_name, index_col=0)

    processed_4form_df['Date_Filed'] = pd.to_datetime(processed_4form_df.Date_Filed)
    # ---------------------------------------------------------------------------------------------------------------

    # Processing of 4form files: Append of Historical Data ------------------------------------------------------------
    path_asset_historical_data = base_path + '/Data/asset_historical_data'

    # Prices and pct_changes ahead: ------------------------------
    pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
                                                look_up_path=path_asset_historical_data,
                                                company_ticket_file_path=path_company_ticket_file)

    # TODO: remove and create one without this
    # Prices and pct_changes ahead (shifted days) ------------------------------
    processed_4form_df = pahp_ri_day.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[126],
                                                                               dates_timedelta_list=[
                                                                                   pd.Timedelta("126 days"),
                                                                                   pd.Timedelta("-126 days")],
                                                                               form4_df=processed_4form_df)

    pahp_ri_day_sahd = ProcessAppendHistoricalPrices(form4_df=processed_4form_df.copy(),
                                                     look_up_path=path_asset_historical_data,
                                                     company_ticket_file_path=path_company_ticket_file)

    processed_4form_df = pahp_ri_day_sahd.append_pct_changes_ahead_in_shifted_dates(periods_ahead_list=[63],
                                                                                    dates_timedelta_list=[
                                                                                        pd.Timedelta("63 days"),
                                                                                        pd.Timedelta("-63 days")],
                                                                                    form4_df=processed_4form_df)
    # ---------------------------------------------------------------------------------------------------------------

    # Save of file ---------------------------------------------------------------------------------------------------
    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_4form_df.to_csv(file_name, index_label='index')
    # ----------------------------------------------------------------------------------------------------------------

    processed_4form_df_loaded = pd.read_csv(file_name, index_col=0)
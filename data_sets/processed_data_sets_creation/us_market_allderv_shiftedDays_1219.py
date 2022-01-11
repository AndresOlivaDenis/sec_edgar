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
    # Loading of data set --------------------------------------------------------------------------------------------
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    ref_file_name = 'us_market_allderv_shiftedDays_1519.py'
    file_name = path_processed_datasets + ref_file_name + ".csv"
    processed_4form_df_1519 = pd.read_csv(file_name, index_col=0)

    path_processed_datasets = base_path + '/Data/processed_datasets/'
    ref_file_name = 'us_market_allderv_shiftedDays_1214.py'
    file_name = path_processed_datasets + ref_file_name + ".csv"
    processed_4form_df_1214 = pd.read_csv(file_name, index_col=0)
    # ----------------------------------------------------------------------------------------------------------------

    # ---------------------------------------------------------------------------------------------------------------
    processed_4form_df = pd.concat([processed_4form_df_1214, processed_4form_df_1519], ignore_index=True)
    # ---------------------------------------------------------------------------------------------------------------

    # Save of file ---------------------------------------------------------------------------------------------------
    path_processed_datasets = base_path + '/Data/processed_datasets/'
    file_name = path_processed_datasets + os.path.basename(__file__) + ".csv"
    processed_4form_df.to_csv(file_name, index_label='index')
    # ----------------------------------------------------------------------------------------------------------------

    processed_4form_df_loaded = pd.read_csv(file_name, index_col=0)

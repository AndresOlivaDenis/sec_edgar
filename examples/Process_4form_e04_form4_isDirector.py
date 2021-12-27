# master_idx_contents Inputs --------------------------------------------------------------------------------------
import os
import numpy as np
import matplotlib.pyplot as plt
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.processing.process_4form_files import Process4FormFiles
from sec_edgar.data.processing.process_append_historical_prices import ProcessAppendHistoricalPrices
from sec_edgar.data.utils.cik_mapping_util import CikMappingUtil
from sec_edgar.stock_historical_data.av_historical_data_request import AVHistoricalDataRequest

base_path = os.path.dirname(os.getcwd())

# Edgar Index content preprocessing ----------------------------------------------------------------------------------

path_company_ticket_file = base_path + "/Data/company_tickers.csv"
cik_mu = CikMappingUtil(company_ticket_file_path=path_company_ticket_file)

companies_symbol_list = ['GOOG', 'INTC', 'AAPL', 'AMD', 'BP', 'V', 'F', 'CVX', 'DAL', 'RCL', 'MSFT', 'FB', 'ADBE',
                         'PEP', 'CSCO', 'SBUX']

companies_symbol_list = ['TSLA', 'JPM', 'HD', 'UNH', 'JNJ', 'PG', 'BAC', 'PFE', 'MA', 'DIS', 'CMCSA', 'NKE',
                         'VZ', 'ABBV', 'KO']

companies_symbol_list = ['XOM', 'COST', 'QCOM', 'DHR', 'DHR', 'WMT', 'WFC', 'LLY', 'MCD', 'MRK', 'INTU', 'TXN',
                         'LOW', 'NEE', 'T', 'LIN', 'UNP', 'UPS', 'ORCL', 'MDT', 'MS', 'HON', 'PM']


companies_cik_list = [cik_mu.get_cik_for_symbol(symbol) for symbol in companies_symbol_list]
year_list = list(range(2015, 2019, 1))
quarter_lists = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
master_idx_contents_ = pre_process_master_idx_content(companies_cik_list=companies_cik_list,
                                                      year_list=year_list,
                                                      quarter_lists=quarter_lists,
                                                      path_out=None,
                                                      merged_file_name=None,
                                                      verbose=True,
                                                      path_raw_default=base_path + '/Data/raw/index')

# ---------------------------------------------------------------------------------------------------------------

# Edgar 4 form files retrieve and preprocessing -----------------------------------------------------------------

form4_df = pre_process_4form_archive_files(master_idx_contents=master_idx_contents_,
                                           path_default_files=base_path + '/Data/raw/files')

# ---------------------------------------------------------------------------------------------------------------

# Processing of 4form files: grouping and define of adjusted transactions ---------------------------------------

p4ff = Process4FormFiles(form4_df,
                         include_derivative_transaction=True,
                         sub_select_dict={'directOrIndirectOwnership': "D",
                                          'isDirector': '1'})

processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
processed_4form_df_ri_day = p4ff.get_transactions_by_day()

# ---------------------------------------------------------------------------------------------------------------


# Processing of 4form files: Append of Historical Data ------------------------------------------------------------

path_asset_historical_data = base_path + '/Data/asset_historical_data'
pahp = ProcessAppendHistoricalPrices(form4_df=form4_df,
                                     look_up_path=path_asset_historical_data,
                                     company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ = pahp.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ = pahp.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                    form4_df=processed_form4_df_)

pahp_ri = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri,
                                        look_up_path=path_asset_historical_data,
                                        company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ri = pahp_ri.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ri = pahp_ri.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                         form4_df=processed_form4_df_ri)

pahp_ri_day = ProcessAppendHistoricalPrices(form4_df=processed_4form_df_ri_day,
                                            look_up_path=path_asset_historical_data,
                                            company_ticket_file_path=path_company_ticket_file)
processed_form4_df_ri_day = pahp_ri_day.append_prices_shifted_ahead(periods_ahead_list=[0, 5, 20])
processed_form4_df_ri_day = pahp_ri_day.append_pct_changes_ahead(periods_ahead_list=[5, 21, 63, 126, 252],
                                                                 form4_df=processed_form4_df_ri_day)

# ---------------------------------------------------------------------------------------------------------------

# Some testing graphs -------------------------------------------------------------------------------------------
if False:
    testing_symbol = "AMD"
    testing_cik = cik_mu.get_cik_for_symbol(testing_symbol)

    tsymb_form4_df_ri_day = processed_form4_df_ri_day[processed_form4_df_ri_day.CIK == testing_cik]
    tsymb_form4_df_ri_day = tsymb_form4_df_ri_day.set_index('Date_Filed')

    historical_data = AVHistoricalDataRequest(symbol=testing_symbol,
                                              api_key="NIAI6K1QQ0KEXACB",
                                              look_up_path=path_asset_historical_data,
                                              update_in_look_up_path=True)

    fig, axs = plt.subplots(2, 1)

    axs[0].stem(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day['transactionSharesAdjust'], linefmt="-.")
    axs[0].grid()

    test_label = 'Shifted Price (0)'
    axs[1].plot(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day[test_label], 'o')
    axs[1].grid()

    historical_data.data_df.index = historical_data.data_df.index.astype('datetime64[ns]')
    sub_data = historical_data.data_df.loc[historical_data.data_df.index > tsymb_form4_df_ri_day.index[0], 'adjusted close']

    axs[1].plot(sub_data.index, sub_data, '-')

    fig, axs = plt.subplots(4, 1)

    axs[0].stem(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day['transactionSharesAdjust'], linefmt="-.")
    axs[0].grid()

    test_label = 'Price pct_change (5)'
    axs[1].stem(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day[test_label])
    axs[1].grid()

    test_label = 'Price pct_change (21)'
    axs[2].stem(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day[test_label])
    axs[2].grid()

    test_label = 'Price pct_change (63)'
    axs[3].stem(tsymb_form4_df_ri_day.index, tsymb_form4_df_ri_day[test_label])
    axs[3].grid()

# ---------------------------------------------------------------------------------------------------------------

label_eval = 'Price pct_change (5)'
form4_df_ri_day_pos = processed_form4_df_ri_day[processed_form4_df_ri_day['transactionSharesAdjust'] > 0]

is_transaction_shares = (form4_df_ri_day_pos['transactionSharesAdjust'] > 0) * 1
is_positive_pct_change = (form4_df_ri_day_pos[label_eval] > 0) * 1

fig, axs = plt.subplots(1, 1)
axs.plot(form4_df_ri_day_pos['transactionSharesAdjust'], form4_df_ri_day_pos[label_eval], 'o')
axs.grid()

# -----------------------------------------------------------------------------------------------

label_eval = 'Price pct_change (63)'

# processed_form4_df_ri_day = processed_form4_df_ri_day.astype({label_eval: float})
form4_df_ri_day_pos = processed_form4_df_ri_day[processed_form4_df_ri_day['transactionSharesAdjust'] > 0]
form4_df_ri_day_neg_eq = processed_form4_df_ri_day[processed_form4_df_ri_day['transactionSharesAdjust'] <= 0]
form4_df_ri_day_eq_zero = processed_form4_df_ri_day[processed_form4_df_ri_day['transactionSharesAdjust'] == 0]

print("All data mean: ", processed_form4_df_ri_day[label_eval].mean())
print("Positive {} mean: ".format(label_eval), form4_df_ri_day_pos[label_eval].mean())
print("Negative equal zero {} mean: ".format(label_eval), form4_df_ri_day_neg_eq[label_eval].mean())
print("Seems Good: {} \n ".format(form4_df_ri_day_pos[label_eval].mean() > processed_form4_df_ri_day[label_eval].mean()))

print("All data pos count: ", ((processed_form4_df_ri_day[label_eval] >= 0.) * 1.).mean())
print("Positive {} mean: ".format(label_eval), ((form4_df_ri_day_pos[label_eval] >= 0.0) * 1.).mean())
print("Negative equal zero {} mean: ".format(label_eval), ((form4_df_ri_day_neg_eq[label_eval] >= 0.0) * 1.).mean())
print("Seems Good: {} \n ".format(((form4_df_ri_day_pos[label_eval] >= 0.0) * 1.).mean() > ((processed_form4_df_ri_day[label_eval] >= 0.) * 1.).mean()  ))

print("Equal Zero {} mean: ".format(label_eval), form4_df_ri_day_eq_zero[label_eval].mean())
print("Equal Zero {} count: ".format(label_eval), ((form4_df_ri_day_eq_zero[label_eval] >= 0.0) * 1.).mean())



import pandas as pd

# TODO: define a funcion for this, and create a notebook


def eval_performance_metric_01(processed_form4_df, label_eval_list):
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
    performance_cik_df_dict = dict()
    labels_performance_df = pd.DataFrame()
    # Returns:
    #   - d
    # TODO
    pass

label_eval = 'Price pct_change (5)'   #, 21, 63, 126, 252
tests_grouping = pd.DataFrame()
tests_grouping['Pos_mean'] = form4_df_ri_day_pos.groupby('CIK')[label_eval].mean()
tests_grouping['Pos data count'] = form4_df_ri_day_pos.groupby('CIK')[label_eval].count()
tests_grouping['all_mean'] = processed_form4_df_ri_day.groupby('CIK')[label_eval].mean()
tests_grouping['all data count'] = processed_form4_df_ri_day.groupby('CIK')[label_eval].count()

tests_grouping['is_greater'] = tests_grouping['Pos_mean'] > tests_grouping['all_mean']
print(tests_grouping)
print(tests_grouping.mean())

# TODO: add confidence interval. Statistically significant evaluation of ">"  NOTEBOOK

# TODO: Study forms, and add filters ¿?
# TODO: eval p4ff.form4_process_0[p4ff.form4_process_0['transactionCode'] == 'P']

if False:
    label_eval = 'Price pct_change (5)'
    threes_hold = 1 / 100
    form4_df_ri_day_pc_pos = processed_form4_df_ri_day[
        processed_form4_df_ri_day['shares_percent_changes_mean'] > threes_hold]

    print("All data mean: ", processed_form4_df_ri_day[label_eval].mean())
    print("form4_df_ri_day_pc_pos {} mean: ".format(label_eval), form4_df_ri_day_pc_pos[label_eval].mean())

    print("All data pos count: ", ((processed_form4_df_ri_day[label_eval] >= 0.0) * 1.0).mean())
    print("form4_df_ri_day_pc_pos {} mean: ".format(label_eval),
          ((form4_df_ri_day_pc_pos[label_eval] >= 0.0) * 1.0).mean())

    form4_df_ri_day_pos[['transactionSharesAdjust', label_eval]]
    np.corrcoef(np.array([form4_df_ri_day_pos[label_eval].values,
                          form4_df_ri_day_pos['transactionSharesAdjust'].values]))

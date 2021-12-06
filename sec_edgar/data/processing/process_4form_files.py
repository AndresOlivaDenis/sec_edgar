# TODO:
"""

- 2nd processings step:
    - Merge multiple (differents owner) transacctions into one:
        At first per day!
        for a given interval of time ?

# Possible reference to validate:
    https://www.secform4.com/insider-trading/50863.htm
    https://www.gurufocus.com/stock/INTC/insider?search=INTC

"""
import pandas as pd
from sec_edgar.data.preprocessing.preprocess_4form_files import pre_process_4form_archive_files
from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content


class Process4FormFiles(object):

    def __init__(self, form4_df, include_derivative_transaction=False):
        self.form4_df = form4_df.copy()

        if include_derivative_transaction:
            sub_select_dict = {'directOrIndirectOwnership': "D"}
        else:
            sub_select_dict = {'transaction_type': "nonDerivativeTransaction",
                               'directOrIndirectOwnership': "D"}

        to_drop_columns = ['documentType',
                           'issuerCik',
                           'transactionTimeliness',
                           'equitySwapInvolved']
        as_type_dict = {'Date_Filed': 'datetime64',
                        'sharesOwnedFollowingTransaction': 'float'}

        self.form4_process_0 = self.init_proccess_form4_df(form4_df,
                                                           sub_select_dict=sub_select_dict,
                                                           to_drop_columns=to_drop_columns,
                                                           as_type_dict=as_type_dict)

    def init_proccess_form4_df(self,
                               form4_df,
                               sub_select_dict,
                               to_drop_columns,
                               as_type_dict):
        form4_process_0 = form4_df.copy()
        for column, value in sub_select_dict.items():
            form4_process_0 = form4_process_0[form4_process_0[column] == value]

        form4_process_0 = form4_process_0.drop(columns=to_drop_columns)
        form4_process_0 = form4_process_0.astype(as_type_dict)

        form4_process_0['transactionSharesAdjust'] = form4_process_0.transactionShares.astype('float')
        is_disposure = form4_process_0.transactionAcquiredDisposedCode == 'D'
        form4_process_0.loc[is_disposure, 'transactionSharesAdjust'] = -form4_process_0.loc[
            is_disposure, 'transactionSharesAdjust']

        # TODO: review this!
        #   Add a way to see if trade is relevant (to avoid examples as DOCU, where buy but actually to low shares amounts! ) % relative to all company shares ?
        #   Share size > 1000 ?
        sOFT = form4_process_0.sharesOwnedFollowingTransaction.astype('float')
        sOFT += is_disposure * 1 * form4_process_0.transactionShares.astype('float')
        form4_process_0['shares_percent_changes'] = form4_process_0.transactionSharesAdjust/sOFT

        form4_process_0['index_id'] = form4_process_0.index.to_list()

        form4_process_0 = form4_process_0.dropna(axis=1, how='all')
        return form4_process_0

    def get_transactions_adjusted(self,
                                  group_by_list,
                                  sort_by='index_id_min',  # 'Date_Filed'
                                  append_unique_of_remaining_columns=False):
        gb_df = self.form4_process_0.groupby(group_by_list)

        processed_df = pd.DataFrame()
        processed_df['index_id'] = gb_df.index_id.unique()
        processed_df['index_id_min'] = gb_df.index_id.min()
        processed_df['n_transactions'] = gb_df.index_id.count()
        processed_df['shares_percent_changes_mean'] = gb_df.shares_percent_changes.mean()
        objective_column = 'transactionSharesAdjust'
        processed_df[objective_column] = gb_df[objective_column].sum()


        # TODO: shares_changes
        # transactionSharesAdjust_sum = gb_df['transactionSharesAdjust'].sum()
        # sharesOwnedFollowingTransaction_adj = gb_df.sharesOwnedFollowingTransaction.sum()
        # less_truth = (transactionSharesAdjust_sum < 0)*1
        # greater_truth = (transactionSharesAdjust_sum > 0) * 1
        # sharesOwnedFollowingTransaction_adj += transactionSharesAdjust_sum * (1*less_truth - 1*greater_truth)
        # processed_df['shares_changes'] = transactionSharesAdjust_sum/sharesOwnedFollowingTransaction_adj

        if append_unique_of_remaining_columns:
            processed_df = self.append_unique_of_remaining_columns(processed_df, gb_df)

        return processed_df.reset_index().sort_values(by=sort_by)

    def append_unique_of_remaining_columns(self, processed_df, gb_df):
        current_columns = list(processed_df.columns) + list(processed_df.index.names)
        remaining_columns = list(self.form4_process_0.columns.difference(current_columns))
        for remaining_column in remaining_columns:
            processed_df[remaining_column] = gb_df[remaining_column].unique()
        return processed_df

    def get_transactions_by_day(self,
                                sort_by='Date_Filed',  # 'Date_filed'
                                append_unique_of_remaining_columns=True):
        group_by_list = ['CIK', 'Company_Names', 'Date_Filed']
        return self.get_transactions_adjusted(group_by_list=group_by_list,
                                              sort_by=sort_by,  # 'Date_filed'
                                              append_unique_of_remaining_columns=append_unique_of_remaining_columns)

    def get_dict_transactions_adjusted_by_owner(self,
                                                sort_by='Date_Filed',  # 'Date_filed'
                                                append_unique_of_remaining_columns=True
                                                ):
        group_by_list = ['CIK', 'Company_Names', 'file_url', 'Date_Filed', 'rptOwnerName']
        processed_df = self.get_transactions_adjusted(group_by_list=group_by_list,
                                                      sort_by=sort_by,  # 'Date_filed'
                                                      append_unique_of_remaining_columns=append_unique_of_remaining_columns)
        processed_df_dict = dict()
        for owner in pd.unique(processed_df['rptOwnerName']):
            processed_df_dict[owner] = processed_df[processed_df['rptOwnerName'] == owner].sort_values(by=sort_by)

        return processed_df_dict

    def get_transactions_by_owner_and_security_tittle(self):
        pass

    def get_transactions_adjusted_by_file_names(self,
                                                sort_by='index_id_min',  # 'Date_filed'
                                                append_unique_of_remaining_columns=True):
        group_by_list = ['CIK', 'Company_Names', 'file_url', 'Date_Filed']
        return self.get_transactions_adjusted(group_by_list=group_by_list,
                                              sort_by=sort_by,  # 'Date_filed'
                                              append_unique_of_remaining_columns=append_unique_of_remaining_columns)

    def plot(self):
        # TODO: plot cummsum & also histograms
        pass



if __name__ == '__main__':
    # master_idx_contents Inputs --------------------------------------------------------------------------------------
    companies_cik_list = ['320193', '1652044', '50863']
    year_list = list(range(2018, 2022, 1))
    quarter_lists = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    master_idx_contents_ = pre_process_master_idx_content(companies_cik_list=companies_cik_list,
                                                          year_list=year_list,
                                                          quarter_lists=quarter_lists,
                                                          path_out=None,
                                                          merged_file_name=None,
                                                          verbose=True)

    # ---------------------------------------------------------------------------------------------------------------

    # form4_df_f = form4_df_f[form4_df_f.transactionPricePerShare.isna().__neg__()]
    # form4_df_f = form4_df_f[form4_df_f.transactionPricePerShare.astype(np.float) > 0]
    # form4_df_f = form4_df_f.dropna(axis=1, how='all')

    # Begining of main_def
    form4_df = pre_process_4form_archive_files(master_idx_contents=master_idx_contents_)
    form4_df_intc = form4_df[form4_df.CIK == '50863']

    p4ff = Process4FormFiles(form4_df, include_derivative_transaction=True)
    processed_4form_df_ri = p4ff.get_transactions_adjusted_by_file_names()
    processed_4form_df_ri_day = p4ff.get_transactions_by_day()

    owner_processed_df_dict = p4ff.get_dict_transactions_adjusted_by_owner()
    df_owner = owner_processed_df_dict['GELSINGER PATRICK P']

    p4ff_ndt = Process4FormFiles(form4_df, include_derivative_transaction=False)
    processed_4form_df_ri_day_ndt = p4ff_ndt.get_transactions_by_day()

    print(processed_4form_df_ri_day.groupby("CIK").transactionSharesAdjust.sum())
    print(processed_4form_df_ri_day_ndt.groupby("CIK").transactionSharesAdjust.sum())

    form4_df_dt = form4_df[form4_df.transaction_type == 'derivativeTransaction']
    print(form4_df_dt.groupby('transactionCode').transactionCode.count())
    # print(form4_df_dt.groupby('securityTitle').securityTitle.unique())





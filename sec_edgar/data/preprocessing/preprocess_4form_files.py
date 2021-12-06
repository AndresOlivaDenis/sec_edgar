import os
import random
import time
import warnings
import numpy as np
import pandas as pd
import  matplotlib.pyplot as plt
import xml.etree.ElementTree as ET

from sec_edgar.data.preprocessing.preprocess_master_idx import pre_process_master_idx_content
from sec_edgar.data.retrieve.retrieve_archive_files import retrieve_and_save_file


def process_form_general_information(form_xml_content):
    general_information = dict()

    for lv_1_element in list(form_xml_content):
        if lv_1_element.tag in ["documentType", "periodOfReport"]:
            general_information[lv_1_element.tag] = lv_1_element.text

        if lv_1_element.tag == "issuer":
            for lv_2_element in list(lv_1_element):
                general_information[lv_2_element.tag] = lv_2_element.text

        if lv_1_element.tag == "reportingOwner":
            for lv_2_element in list(lv_1_element):
                if lv_2_element.tag == "reportingOwnerId":
                    for lv_3_element in list(lv_2_element):
                        if lv_3_element.tag == "rptOwnerName":
                            general_information[lv_3_element.tag] = lv_3_element.text
                if lv_2_element.tag == "reportingOwnerRelationship":
                    for lv_3_element in list(lv_2_element):
                        general_information[lv_3_element.tag] = lv_3_element.text
        if lv_1_element.tag == "ownerSignature":
            for lv_2_element in list(lv_1_element):
                if lv_2_element.tag == "signatureDate":
                    general_information[lv_2_element.tag] = lv_2_element.text

    return general_information.copy()


def process_form_securities_tables(form_xml_content):
    def process_all(xml_element, parent_name=None):
        has_inner_elements = list(xml_element) != []
        if not has_inner_elements:
            if xml_element.tag == "value":
                return {parent_name: xml_element.text}
            else:
                return {xml_element.tag: xml_element.text}
        else:
            inner_elements_contents = {}
            for inner_element in list(xml_element):
                inner_elements_contents = {**inner_elements_contents,
                                           **process_all(inner_element, parent_name=xml_element.tag)}
            return inner_elements_contents

    non_derivative_transactions_list = []
    for lv_1_element in list(form_xml_content):
        if lv_1_element.tag in ["nonDerivativeTable", "derivativeTable"]:
            for transaction_element in list(lv_1_element):
                transaction_dict = {"transaction_type": transaction_element.tag,
                                    **process_all(transaction_element, parent_name=None)}
                non_derivative_transactions_list.append(transaction_dict)
    return non_derivative_transactions_list


def pre_process_4form_archive_files(master_idx_contents,
                                    path_default_files=os.path.dirname(
                                        os.path.dirname(os.path.dirname(os.getcwd()))) + '/Data/raw/files',
                                    ):
    master_idx_contents_4form = master_idx_contents[master_idx_contents.Form_Type.isin(['4'])].reset_index(drop=True)

    documents_content_list = []

    for index, row in master_idx_contents_4form.iterrows():
        print("\nLoading {}/{} ({:.2f}%)".format(index + 1, len(master_idx_contents_4form),
                                                 100 * (index + 1) / len(master_idx_contents_4form)))

        directory = os.path.join(path_default_files, os.path.join(str(row.year), row.quarter))
        directory = os.path.join(directory, os.path.join(row.CIK, row.Form_Type))
        file_name = row.Filename.split("/")[-1]
        path_file = os.path.join(directory, file_name)

        print("\t Archive file name: {}".format(path_file))

        file_exist = os.path.isfile(path_file)
        if file_exist:
            print("\t File exist on path_default_files, skip retrieving and saving.")
        if not file_exist:
            sleep_interval = [0.5, 2]

            print("\t File does not exist on path_default_files, retrieving and saving.")
            retrieve_and_save_file(row=row,
                                   path_default_files=path_default_files,
                                   verbose=True)
            sleep_time = random.random() * (sleep_interval[1] - sleep_interval[0]) + sleep_interval[0]
            print("\t Save complete, going to sleep for about: {} [s]".format(sleep_time))
            time.sleep(sleep_time)

        pre_size = len(documents_content_list)
        with open(path_file) as f:
            print("\t Opening file")
            file_content = f.read()
            content_split_1 = file_content.split("</SEC-HEADER>")
            sec_header = content_split_1[0]
            sec_documents = content_split_1[1].split("</SEC-DOCUMENT>")[0]
            documents = sec_documents.split("</DOCUMENT>")
            for document in documents:
                if "<XML>" in document:
                    print("\t Reading xml form content")
                    xml_split = document.split("<XML>")
                    non_xml_content = xml_split[0]
                    xml_content = xml_split[1].split("</XML>")[0]
                    for line in non_xml_content.split("\n"):
                        if "<TYPE>" in line:
                            document_type = line.replace("<TYPE>", "")
                        if "<FILENAME>" in line:
                            document_file_name = line.replace("<FILENAME>", "")
                        if "<DESCRIPTION>" in line:
                            document_description = line.replace("<DESCRIPTION>", "")
                    if xml_content[0:1] == '\n':
                        xml_content = xml_content[1:]

                    root_xml = ET.fromstring(xml_content)
                    general_information = process_form_general_information(root_xml)
                    securities_list = process_form_securities_tables(root_xml)

                    print("\t Number of secutiries in form: {}".format(len(securities_list)))

                    # for each derivative and non derivative transaction:
                    for securities in securities_list:
                        documents_content_list.append({**row.to_dict(),
                                                       **dict(doc_name=file_name.replace(".txt", ""),
                                                              file_url='https://www.sec.gov/Archives/' + file_name),
                                                       **general_information,
                                                       **securities})
        if len(documents_content_list) == pre_size:
            warnings.simplefilter("always")
            warnings.warn("\n File preprocessing does not added new secutiries. TODO: REVIEW! ", UserWarning)
        print("\t Preprocess of Archive file complete.")

    return pd.DataFrame(documents_content_list)


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

    form4_df_f = form4_df[form4_df.transaction_type == "nonDerivativeTransaction"]
    form4_df_f = form4_df_f[form4_df_f.directOrIndirectOwnership == "D"]

    to_drop_columns = ['documentType', 'issuerCik', 'transactionTimeliness', 'equitySwapInvolved']
    form4_df_f = form4_df_f.drop(columns=to_drop_columns)

    form4_df_f = form4_df_f.dropna(axis=1, how='all')

    form4_df_f['form4_df_f'] = form4_df_f.Date_Filed.astype('datetime64')
    form4_df_f['transactionSharesAdjust'] = form4_df_f.transactionShares.astype('int')
    is_disposure = form4_df_f.transactionAcquiredDisposedCode == 'D'
    form4_df_f.loc[is_disposure, 'transactionSharesAdjust'] = -form4_df_f.loc[is_disposure, 'transactionSharesAdjust']
    form4_df_f.index_id = form4_df_f.index.to_list()

    # TODO: Create:
    #   transactionSharesAdjust: To float and Adjust by transactionAcquiredDisposed Code
    #   as_type conversion ? (transactionShares, sharesOwnedFollowingTransaction)
    #   % of transacctions (maybe after grouping)

    index_columns = ['CIK', 'Company_Names', 'Form_Type', 'Date_Filed', 'Filename', 'year',
                     'quarter', 'doc_name', 'periodOfReport', 'issuerName',
                     'issuerTradingSymbol', 'rptOwnerName',
                     # 'isDirector', 'isOfficer', 'isTenPercentOwner', 'isOther', 'officerTitle', # TODO: move to unique
                     'signatureDate', 'transaction_type',  # 'transactionDate',
                     # 'transactionFormType',
                     'directOrIndirectOwnership']  # TODO: review non of this are nan-values (since they are removed!)
    form4_df_f_i = form4_df_f.set_index(index_columns)

    # Review if gruop by and is nan, them is removed!
    form4_df_f_gb = form4_df_f.groupby(index_columns)
    processed_4form_df = pd.DataFrame()
    processed_4form_df['transactionCode'] = form4_df_f_gb.transactionCode.unique()
    processed_4form_df['transactionDate'] = form4_df_f_gb.transactionDate.unique()
    processed_4form_df['transactionFormType'] = form4_df_f_gb.transactionFormType.unique()
    processed_4form_df['securityTitle'] = form4_df_f_gb.securityTitle.unique()
    processed_4form_df['transactionAcquiredDisposedCode'] = form4_df_f_gb.transactionAcquiredDisposedCode.unique()
    processed_4form_df['transactionShares'] = form4_df_f_gb.transactionShares.unique()
    processed_4form_df['sharesOwnedFollowingTransaction'] = form4_df_f_gb.sharesOwnedFollowingTransaction.unique()
    processed_4form_df['transactionPricePerShare'] = form4_df_f_gb.transactionPricePerShare.unique()
    processed_4form_df['transactionSharesAdjust_'] = form4_df_f_gb.transactionSharesAdjust.unique()
    processed_4form_df['transactionSharesAdjust'] = form4_df_f_gb.transactionSharesAdjust.sum()


    # TODO: COlumns to analise when grouping:
    #   transactionCode : unique()
    #   transactionShares : unique()
    #   transactionPricePerShare : unique()
    #   transactionAcquiredDisposedCode : unique()
    #   securityTitle : unique()
    #   sharesOwnedFollowingTransaction : Keep last One ?
    #   News:
    #   transactionSharesAdjust : sum()
    #   transactions_count : sum() # Create!

    transactions_count_series = form4_df_f_gb.count().transactionShares
    transactions_count_series.name = 'transactions_count'
    processed_4form_df['transactions_count'] = transactions_count_series

    processed_4form_df_ri = processed_4form_df.reset_index()
    # TODO: To Review. (Goal is try to joint same files ?)
    print("processed_4form_df_ri.doc_name.duplicated().any(): ", processed_4form_df_ri.doc_name.duplicated().any())
    duplicated_ones = processed_4form_df_ri[processed_4form_df_ri.doc_name.duplicated(keep=False)]
    # TO review: INTC is being removed! (maybe by group_by ?)

    print(pd.unique(form4_df.Date_Filed))
    print(print(pd.unique(form4_df.transactionCode)))
    print(pd.unique(form4_df.securityTitle))
    print(form4_df_f.groupby("securityTitle").count().CIK)
    print(form4_df_f.groupby("transactionCode").count().CIK)

    processed_4form_df_ri[processed_4form_df_ri.CIK == '50863'].transactionSharesAdjust.cumsum().plot()
    processed_4form_df_ri[processed_4form_df_ri.CIK == '1652044'].transactionSharesAdjust.cumsum().plot()
    processed_4form_df_ri[processed_4form_df_ri.CIK == '320193'].transactionSharesAdjust.cumsum().plot()


    for cik in companies_cik_list:
        plt.figure()
        a = processed_4form_df_ri[processed_4form_df_ri.CIK == cik].transactionSharesAdjust.cumsum()
        a.index = processed_4form_df_ri[processed_4form_df_ri.CIK == cik].Date_Filed
        a.plot()
    # Prev Approach:
    #   Insiders all shares evolution.

    # TODO: another possible processing approach:
    #       BY rptOwnerName, and actually tracks evoluation on how many shares does he/she have!
    #           Gropu by rptOwnerName, COmpany and Date_Filed ?
    #               Take shares OwnedFOllowingTransaction ?
    #           Do it by differents type of securityTitle ?
    #           Sort by Date_Filed, transactionDate, index (inverse of, if it guarrantee that they are in order!)


    # Actually: Group by, and them merge ?
    # form4_df_f.groupby(index_columns).transactionCode.unique()

    # TO look at:
    # https: // www.sec.gov / about / forms / form4data.pdf
    # https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-69


    form4_df_f_dt = form4_df[form4_df.transaction_type == "derivativeTransaction"]
    form4_df_f_dt = form4_df_f_dt[form4_df_f_dt.directOrIndirectOwnership == "D"]

    to_drop_columns = ['documentType', 'issuerCik', 'transactionTimeliness', 'equitySwapInvolved']
    form4_df_f_dt = form4_df_f_dt.drop(columns=to_drop_columns)

    form4_df_f_dt = form4_df_f_dt.dropna(axis=1, how='all')
    form4_df_f_dt['transactionSharesAdjust'] = form4_df_f_dt.transactionShares.astype('float')
    form4_df_f_dt['form4_df_f'] = form4_df_f_dt.Date_Filed.astype('datetime64')
    is_disposure = form4_df_f_dt.transactionAcquiredDisposedCode == 'D'
    form4_df_f_dt.loc[is_disposure, 'transactionSharesAdjust'] = -form4_df_f_dt.loc[is_disposure, 'transactionSharesAdjust']

    # TODO:
    #   Create a class
    #



    to_drop_columns = ['documentType', 'issuerCik', 'transactionTimeliness', 'equitySwapInvolved']
    form4_df_f = form4_df_f.drop(columns=to_drop_columns)


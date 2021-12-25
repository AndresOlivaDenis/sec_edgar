import os
import pandas as pd


class CikMappingUtil(object):

    def __init__(self, company_ticket_file_path=os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
                                                    + '/Data/company_tickers.csv'):
        self.company_ticket_file_path = company_ticket_file_path
        self.company_ticket_df = pd.read_csv(company_ticket_file_path)

    def get_symbol_for_cik(self, cik):
        """
        Integer or string.
        """
        cik = int(cik)
        return self.company_ticket_df[self.company_ticket_df.cik_str == cik].ticker.iloc[0]

    def get_cik_for_symbol(self, symbol):
        return str(self.company_ticket_df[self.company_ticket_df.ticker == symbol].cik_str.iloc[0])


if __name__ == '__main__':
    cik_mu = CikMappingUtil()

    companies_cik_list = ['320193', '1652044', '50863']
    for cik_ in companies_cik_list:
        print(cik_mu.get_symbol_for_cik(cik_))

    companies_symbol_list = ['AMD', 'INTC', 'BABA']
    for symbol_ in companies_symbol_list:
        print(cik_mu.get_cik_for_symbol(symbol_))


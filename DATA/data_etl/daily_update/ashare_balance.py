import datetime

import pandas as pd
from tqdm import tqdm
from xtquant import xtdata

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube


def extract_ashare_balance_daily(start_date: str, end_date: str):
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=['balance'], start_time=start_date,
                                                   end_time=end_date,
                                                   report_type='report_time')
        balance_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['balance'])
            financial_ori_df['ticker'] = ticker
            balance_df = pd.concat([balance_df, financial_ori_df], axis=0)
        if balance_df.empty:
            logger_datacube.info(f'[DAILY] ashare_balance {start_date}-{end_date}:未更新')
            return
        balance_df = balance_df.rename(columns={'m_stateTypeCode': 'm_state_typecode',
                                                'm_coverPeriod': 'm_cover_period',
                                                'm_industryCode': 'm_industry_code',
                                                'm_balanceCurrency': 'm_balance_currency',
                                                'm_cashAdepositsCentralBank': 'm_cash_a_deposits_central_bank',
                                                'm_nobleMetal': 'm_noble_metal',
                                                'm_depositsOtherFinancialInstitutions': 'm_deposits_other_financial_institutions',
                                                'm_currentInvestment': 'm_current_investment',
                                                'm_redemptoryMonetaryCapitalSale': 'm_redemptory_monetary_capital_sale',
                                                'm_netAmountSubrogation': 'm_net_amount_subrogation',
                                                'm_refundableDeposits': 'm_refundable_deposits',
                                                'm_netAmountLoanPledged': 'm_net_amount_loan_pledged',
                                                'm_fixedTimeDeposit': 'm_fixed_time_deposit',
                                                'm_netLongtermDebtInvestments': 'm_net_longterm_debt_investments',
                                                'm_permanentInvestment': 'm_permanent_investment',
                                                'm_depositForcapitalRecognizance': 'm_deposit_for_capital_recognizance',
                                                'm_netBalConstructionProgress': 'm_net_bal_construction_progress',
                                                'm_separateAccountAssets': 'm_separate_account_assets',
                                                'm_capitalInvicariousBussiness': 'm_capital_invicarious_bussiness',
                                                'm_otherAssets': 'm_other_assets',
                                                'm_depositsWithBanksOtherFinancialIns': 'm_deposits_with_banks_other_financial_ins',
                                                'm_indemnityPayable': 'm_indemnity_payable',
                                                'm_policyDividendPayable': 'm_policy_dividend_payable',
                                                'm_guaranteeInvestmentFunds': 'm_guarantee_investment_funds',
                                                'm_premiumsReceivedAdvance': 'm_premiums_received_advance',
                                                'm_insuranceLiabilities': 'm_insurance_liabilities',
                                                'm_liabilitiesIndependentAccounts': 'm_liabilities_independent_accounts',
                                                'm_liabilitiesVicariousBusiness': 'm_liabilities_vicarious_business',
                                                'm_otherLiablities': 'm_other_liabilities',
                                                'm_capitalPremium': 'm_capital_premium',
                                                'm_petainedProfit': 'm_petained_profit',
                                                'm_provisionTransactionRisk': 'm_provision_transaction_risk',
                                                'm_otherReserves': 'm_other_reserves'})
        # 插入数据库
        insert_df_to_postgres(balance_df, table_name='ashare_balance')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert ashare_balance,{start_date}-{end_date} ,cost: {end_time - start_time},lens={len(balance_df)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_balance:{err}')
        return


if __name__ == '__main__':
    extract_ashare_balance_daily(start_date='20230330', end_date='20230330')

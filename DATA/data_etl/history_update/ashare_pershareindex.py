import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import  insert_df_to_postgres

'''
A股股票每股指标表
数据来源:xtdata
'''
def extract_stock_pershareindex_history():
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=[],
                                                   start_time='', end_time='',report_type='report_time')
        pershareindex_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['PershareIndex'])
            financial_ori_df['ticker'] = ticker
            pershareindex_df = pd.concat([pershareindex_df, financial_ori_df], axis=0)
        pershareindex_df = pershareindex_df.rename(columns={
            'm_stateTypeCode':'m_state_typecode',
            'm_coverPeriod':'m_cover_period',
            'm_industryCode':'m_industry_code',
            'm_netinterestpershareindex': 'm_net_interest_pershareindex',
            'm_netFeesCommissions': 'm_net_fees_commissions',
            'm_insuranceBusiness': 'm_insurance_business',
            'm_separatePremium': 'm_separate_premium',
            'm_asideReservesUndueLiabilities': 'm_aside_reserves_undue_liabilities',
            'm_paymentsInsuranceClaims': 'm_payments_insurance_claims',
            'm_amortizedCompensationExpenses': 'm_amortized_compensation_expenses',
            'm_netReserveInsuranceLiability': 'm_net_reserve_insurance_liability',
            'm_policyReserve': 'm_policy_reserve',
            'm_amortizeInsuranceReserve': 'm_amortize_insurance_reserve',
            'm_nsuranceFeesCommissionExpenses': 'm_nsurance_fees_commission_expenses',
            'm_operationAdministrativeExpense': 'm_operation_administrative_expense',
            'm_amortizedReinsuranceExpenditure': 'm_amortized_reinsurance_expenditure',
            'm_netProfitLossdisposalNonassets': 'm_net_profit_loss_disposal_nonassets',
            'm_otherItemsAffectingNetProfit': 'm_other_items_affecting_net_profit'
        })
        # 插入数据库
        insert_df_to_postgres(pershareindex_df, table_name='ashare_pershareindex')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] ashare_pershareindex:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully insert ashare_pershareindex ,cost: {end_time - start_time} ,lens={len(pershareindex_df)}')


if __name__ == '__main__':
    extract_stock_pershareindex_history()

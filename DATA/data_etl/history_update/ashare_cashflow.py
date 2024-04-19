import datetime

import pandas as pd
from tqdm import tqdm
from xtquant import xtdata

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube


def extract_stock_cashflow_history():
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=['cashflow'], start_time='', end_time='',
                                                   report_type='report_time')
        cashflow_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['cashflow'])
            financial_ori_df['ticker'] = ticker
            cashflow_df = pd.concat([cashflow_df, financial_ori_df], axis=0)
        cashflow_df = cashflow_df.rename(columns={'m_stateTypeCode': 'm_state_typecode',
                                                  'm_coverPeriod': 'm_cover_period',
                                                  'm_industryCode': 'm_industry_code',
                                                  'm_cashSellingProvidingServices': 'm_cash_selling_providing_services',
                                                  'm_netDecreaseUnwindingFunds': 'm_net_decrease_unwinding_funds',
                                                  'm_netReductionPurchaseRebates': 'm_net_reduction_purchase_rebates',
                                                  'm_netIncreaseDepositsBanks': 'm_net_increase_deposits_banks',
                                                  'm_netCashReinsuranceBusiness': 'm_net_cash_reinsurance_business',
                                                  'm_netReductionDeposInveFunds': 'm_net_reduction_depos_inve_funds',
                                                  'm_netIncreaseUnwindingFunds': 'm_net_increase_unwinding_funds',
                                                  'm_netReductionAmountBorrowedFunds': 'm_net_reduction_amount_borrowed_funds',
                                                  'm_netReductionSaleRepurchaseProceeds': 'm_net_reduction_sale_repurchase_proceeds',
                                                  'm_investmentPaidInCash': 'm_investment_paid_in_cash',
                                                  'm_paymentOtherCashRelated': 'm_payment_other_cash_related',
                                                  'm_cashOutFlowsInvesactivities': 'm_cash_outflows_invesactivities',
                                                  'm_absorbCashEquityInv': 'm_absorb_cash_equity_inv',
                                                  'm_otherImpactsOnCash': 'm_other_impacts_on_cash',
                                                  'm_addOperatingReceivableItems': 'm_add_operating_receivable_items'
                                                  })
        # 插入数据库
        insert_df_to_postgres(cashflow_df, table_name='ashare_cashflow')
    except Exception as err:
        logger_datacube.error(f'[ERROR]:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully inserted financial cashflow data,cost time: {end_time - start_time},lens={len(cashflow_df)}')


if __name__ == '__main__':
    extract_stock_cashflow_history()

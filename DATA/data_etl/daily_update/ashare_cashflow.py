import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import PostgresClient, insert_df_to_postgres
from config.conf import today_str


def extract_ashare_cashflow_daily(start_date: str, end_date: str):
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=['cashflow'], start_time=start_date, end_time=end_date,
                                                   report_type='report_time')
        cashflow_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['cashflow'])
            financial_ori_df['ticker'] = ticker
            cashflow_df = pd.concat([cashflow_df, financial_ori_df], axis=0)
        if cashflow_df.empty:
            logger_datacube.info(f'[DAILY] ashare_cashflow:{start_date}-{end_date} :未更新')
            return

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
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[Daily] Successfully insert ashare_cashflow  data,{start_date}-{end_date} ,cost:{end_time - start_time},lens={len(cashflow_df)}')

    except Exception as err:
        logger_datacube.error(f'[ERROR]:{err}')
        return

if __name__ == '__main__':
    extract_ashare_cashflow_daily(start_date=today_str,end_date=today_str)

import datetime

import akshare as ak
import pandas as pd

from Config.conf import today_format
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import add_exchange_suffix, \
    convert_to_datetime

'''
A股新股历史数据
数据来源：akshare,东方财富
'''


def extract_ashare_new_issue_history(start_date=None, end_date=None):
    try:
        start_time = datetime.datetime.now()
        new_issue_df = ak.stock_xgsglb_em(symbol="全部股票")

        new_issue_df = new_issue_df[
            ['股票代码', '股票简称', '申购代码', '发行总数', '网上发行', '顶格申购需配市值', '申购上限', '发行价格',
             '首日收盘价', '申购日期', '中签号公布日', '中签缴款日期', '上市日期', '发行市盈率', '行业市盈率',
             '中签率', '询价累计报价倍数', '配售对象报价家数', '连续一字板数量', '涨幅', '每中一签获利']]

        new_issue_df = new_issue_df.rename(
            columns={'股票代码': 'ticker', '股票简称': 'ticker_name', '申购代码': 'purchase_code',
                     '发行总数': 'total_issue', '网上发行': 'online_issue',
                     '顶格申购需配市值': 'top_purchase_need_market_value',
                     '申购上限': 'purchase_limit', '发行价格': 'issue_price',
                     '首日收盘价': 'first_day_close_price', '申购日期': 'purchase_date',
                     '上市日期': 'issue_date', '发行市盈率': 'issue_pe',
                     '行业市盈率': 'industry_pe', '中签率': 'lucky_number_rate',
                     '询价累计报价倍数': 'inquiry_accumulated_quotation_multiple',
                     '配售对象报价家数': 'number_of_bidders', '连续一字板数量': 'up_limit_board_number',
                     '涨幅': 'increase', '每中一签获利': 'profit_per_lucky_number'})
        new_issue_df['ticker'] = new_issue_df['ticker'].apply(add_exchange_suffix)
        new_issue_df['issue_date'] = pd.to_datetime(new_issue_df['issue_date'])
        new_issue_df['purchase_date'] = pd.to_datetime(new_issue_df['purchase_date'])
        if start_date or end_date:
            new_issue_df = new_issue_df[
                (new_issue_df['purchase_date'] >= convert_to_datetime(start_date)) & (
                        new_issue_df['purchase_date'] <= convert_to_datetime(end_date))]
        if new_issue_df.empty:
            logger_datacube.info(f'[HISTORY]time range:{start_date} to {end_date} new issue data:未更新')
            return
        insert_df_to_postgres(new_issue_df, table_name='ashare_new_issue')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[HISTORY]successfully insert ashare_new_issue,cost time:{end_time - start_time},lens={len(new_issue_df)}')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] ashare_new_issue:{err}')
        return


if __name__ == '__main__':
    extract_ashare_new_issue_history(start_date='2024-01-01', end_date=today_format)

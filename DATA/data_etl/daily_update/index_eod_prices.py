import datetime

import akshare as ak
import pandas as pd
from Utils.logger import logger_datacube
from Utils.Database_connector import insert_df_to_postgres
from Utils.utils import convert_ticker_to_sina_format, convert_to_datetime
from config.conf import today_format


def exstract_index_eod_prices_daily(start_date, end_date):
    list_all = ['000001.SH',
                '000011.SH',
                '000016.SH',
                '000300.SH',
                '000688.SH',
                '000852.SH',
                '000905.SH',
                '000985.SH',
                '399001.SZ',
                '399006.SZ',
                '399100.SZ',
                '399106.SZ']
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()
    start_time = datetime.datetime.now()
    try:
        dfs = []
        for index_code in list_all:
            symbol = convert_ticker_to_sina_format(index_code)
            index_price_ori = ak.stock_zh_index_daily(symbol)
            index_price_ori['index_code'] = index_code
            index_price_ori=index_price_ori.rename(columns={'date':'datetime'})
            index_price_ori = index_price_ori[(index_price_ori['datetime'] >= start_date) & (index_price_ori['datetime'] <= end_date)]
            dfs.append(index_price_ori)
        index_eod_prices = pd.concat(dfs, ignore_index=True)
        insert_df_to_postgres(index_eod_prices, table_name='index_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY]successfully insert index_eod_prices,{start_date}-{end_date},cost time = {end_time-start_time},lens={len(index_eod_prices)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] index_eod_prices:{err}')
        return

if __name__ == '__main__':
    exstract_index_eod_prices_daily(start_date=today_format, end_date=today_format)
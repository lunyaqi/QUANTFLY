import datetime

import akshare as ak
import pandas as pd

from Config.conf import today_format, zz_index_list, gz_index_list
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_ticker_to_sina_format, convert_to_datetime


def exstract_index_eod_prices_daily(start_date, end_date):
    list_all = zz_index_list + gz_index_list
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()
    start_time = datetime.datetime.now()
    try:
        dfs = []
        for index_code in list_all:
            symbol = convert_ticker_to_sina_format(index_code)
            index_price_ori = ak.stock_zh_index_daily(symbol)
            index_price_ori['index_code'] = index_code
            index_price_ori = index_price_ori.rename(columns={'date': 'datetime'})
            index_price_ori = index_price_ori[
                (index_price_ori['datetime'] >= start_date) & (index_price_ori['datetime'] <= end_date)]
            dfs.append(index_price_ori)
        index_eod_prices = pd.concat(dfs, ignore_index=True)
        if index_eod_prices.empty:
            logger_datacube.warning(f'[DAILY] index_eod_prices 未更新')
        insert_df_to_postgres(index_eod_prices, table_name='index_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY]successfully insert index_eod_prices,{start_date}-{end_date},cost time = {end_time - start_time},lens={len(index_eod_prices)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] index_eod_prices:{err}')
        return


if __name__ == '__main__':
    exstract_index_eod_prices_daily(start_date=today_format, end_date=today_format)

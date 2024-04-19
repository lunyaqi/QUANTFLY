import datetime

import akshare as ak
import pandas as pd

from Config.conf import gz_index_list, zz_index_list
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_ticker_to_sina_format

'''
指数日行情
数据来源: akshare,国证指数网站，中证指数网站
'''


def exstract_index_eod_prices_history():
    start_time = datetime.datetime.now()
    list_all = gz_index_list + zz_index_list
    try:
        dfs = []
        for index_code in list_all:
            symbol = convert_ticker_to_sina_format(index_code)
            index_price_ori = ak.stock_zh_index_daily(symbol)
            index_price_ori['index_code'] = index_code

            index_price_ori = index_price_ori.rename(columns={'date': 'datetime'})
            dfs.append(index_price_ori)
        index_eod_prices = pd.concat(dfs, ignore_index=True)
        insert_df_to_postgres(index_eod_prices, table_name='index_eod_prices')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] index_eod_prices:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully insert index_eod_prices data,cost time = {end_time - start_time},lens={len(index_eod_prices)}')


if __name__ == '__main__':
    exstract_index_eod_prices_history()

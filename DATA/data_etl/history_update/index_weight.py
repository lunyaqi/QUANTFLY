import datetime

import akshare as ak
import pandas as pd

from Config.conf import zz_index_list, gz_index_list
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_datetime_column_format, add_exchange_suffix

'''
指数权重
数据来源: akshare,国证指数网站，中证指数网站
'''


def _get_zz_index_weight(zz_index_list):
    zz_index_dfs = []
    for index_code in zz_index_list:
        index_symbol = index_code.split('.')[0]
        zz_index_df = ak.index_stock_cons_weight_csindex(symbol=index_symbol)
        zz_index_df['指数代码'] = index_code
        zz_index_dfs.append(zz_index_df)

    zz_index_df_all = pd.concat(zz_index_dfs, ignore_index=True)
    zz_index_df_all = zz_index_df_all[['日期', '指数代码', '指数名称', '成分券代码', '权重']]
    zz_index_df_all.columns = ['datetime', 'index_code', 'index_name', 'ticker', 'weight']
    zz_index_df_all = convert_datetime_column_format(zz_index_df_all)
    zz_index_df_all['ticker'] = zz_index_df_all['ticker'].apply(add_exchange_suffix)
    return zz_index_df_all


def _get_gz_index_weight(gz_index_list):
    gz_index_dfs = []
    for index_code in gz_index_list:
        index_symbol = index_code.split('.')[0]
        gz_index_df = ak.index_detail_hist_cni(symbol=index_symbol)
        gz_index_df['指数代码'] = index_code
        if index_symbol == '399006':
            gz_index_df['指数名称'] = '创业板指数'
        elif index_symbol == '399106':
            gz_index_df['指数名称'] = '深证综指'
        elif index_symbol == '399001':
            gz_index_df['指数名称'] = '深证成指'
        gz_index_dfs.append(gz_index_df)

    gz_index_df_all = pd.concat(gz_index_dfs, ignore_index=True)
    gz_index_df_all = gz_index_df_all[['日期', '指数代码', '指数名称', '样本代码', '权重']]
    gz_index_df_all.columns = ['datetime', 'index_code', 'index_name', 'ticker', 'weight']
    gz_index_df_all = convert_datetime_column_format(gz_index_df_all)
    gz_index_df_all['ticker'] = gz_index_df_all['ticker'].apply(add_exchange_suffix)
    return gz_index_df_all


def extract_index_weight_history():
    start_time = datetime.datetime.now()
    try:
        zz_index_weight = _get_zz_index_weight(zz_index_list)
        gz_index_weight = _get_gz_index_weight(gz_index_list)
        index_weight_all = pd.concat([zz_index_weight, gz_index_weight], ignore_index=True)

        # 插入数据库

        insert_df_to_postgres(index_weight_all, table_name='index_weight')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] index_weight:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully insert index_weight,cost: {end_time - start_time},lens={len(index_weight_all)}')


if __name__ == '__main__':
    extract_index_weight_history()

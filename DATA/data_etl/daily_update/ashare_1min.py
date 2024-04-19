import datetime

import pandas as pd
from xtquant import xtdata
from Config.conf import today_str
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube


def chunk_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def extract_ashare_1min_daily(date):
    # 仅支持获取一天数据
    # 每天 每个ticker 一共241条数据
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        dfs = []
        for ticker in ashare_list:
            # 获取不复权历史数据
            price_ori = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1m', start_time=date,
                                              end_time=date, count=-1, dividend_type='none', fill_data=True)
            price_adj = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1m', start_time=date,
                                              end_time=date, count=-1, dividend_type='back', fill_data=True)

            price_ori_df = pd.DataFrame(price_ori[ticker]).reset_index()

            price_adj_df = pd.DataFrame(price_adj[ticker]).reset_index()
            price_ori_df = price_ori_df.rename(
                columns={'index': 'timestamp', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                         'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
            price_adj_df = price_adj_df.rename(
                columns={'index': 'timestamp', 'open': 'open_adj', 'high': 'high_adj', 'low': 'low_adj',
                         'close': 'close_adj', 'preClose': 'pre_close_adj'})
            price_adj_df = price_adj_df[['timestamp', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'pre_close_adj']]

            tmp_price_df = pd.merge(price_adj_df, price_ori_df, on=['timestamp'])
            tmp_price_df['ticker'] = ticker
            dfs.append(tmp_price_df)
        price_df = pd.concat(dfs, axis=0)

        if len(price_df) <= 1:
            raise ValueError(f'{date}数据未更新!')

        del price_df['time']
        price_df['datetime'] = pd.to_datetime(price_df['timestamp'].astype(str).str[:8], format='%Y%m%d')
        price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
        # 插入数据库
        insert_df_to_postgres(price_df, table_name='ashare_1min')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert ashare_1min,{date},cost: {end_time - start_time}')
    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_1min:{err}')


if __name__ == '__main__':
    extract_ashare_1min_daily(today_str)

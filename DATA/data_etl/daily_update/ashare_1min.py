import csv

import akshare as ak
import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import PostgresClient, insert_df_to_postgres
from Utils.utils import convert_to_datetime
from config.conf import today_int, today_str
import io


def chunk_list(lst, chunk_size):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]


def extract_ashare_1min_daily(date):
    # 仅支持获取一天数据
    # 每天 每个ticker 一共241条数据
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        ticker_lists = chunk_list(ashare_list,100)
        for ticker_list in ticker_lists:
            price_ori = xtdata.get_market_data(field_list=[], stock_list=ticker_list, period='1m', start_time=date,
                                               end_time=date, count=-1, dividend_type='none', fill_data=True)
            price_ori_dfs = [pd.DataFrame(value).stack() for value in price_ori.values()]
            price_ori_df = pd.concat(price_ori_dfs, axis=1)
            if len(price_ori_df.columns) <= 1:
                raise ValueError(f'{date}数据未更新!')
            price_ori_df.columns = price_ori.keys()
            price_ori_df = price_ori_df.reset_index()
            price_ori_df = price_ori_df.rename(
                columns={'level_0': 'ticker','level_1':'timestamp', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                         'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
            # 获取后复权历史数据
            price_adj = xtdata.get_market_data(field_list=[], stock_list=ticker_list, period='1m', start_time=date,
                                              end_time=date, count=-1,dividend_type='back', fill_data=True)
            price_adj_dfs = [pd.DataFrame(value).stack() for value in price_adj.values()]
            price_adj_df = pd.concat(price_adj_dfs, axis=1)
            price_adj_df.columns = price_adj.keys()
            price_adj_df = price_adj_df.reset_index()
            price_adj_df = price_adj_df.rename(
                columns={'level_0': 'ticker','level_1':'timestamp',  'open': 'open_adj', 'high': 'high_adj', 'low': 'low_adj', 'close': 'close_adj',
                         'preClose': 'pre_close_adj'})
            price_adj_df = price_adj_df[
                ['timestamp', 'ticker', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'pre_close_adj']]
            price_df = pd.merge(price_adj_df, price_ori_df, on=['timestamp', 'ticker'])
            del price_df['time']
            price_df['datetime'] = pd.to_datetime(price_df['timestamp'].astype(str).str[:8], format='%Y%m%d')
            price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
            # 插入数据库
            insert_df_to_postgres(price_df, table_name='ashare_1min')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert ashare_1min_prices,{date},cost: {end_time - start_time}')
    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_1min_prices:{err}')


if __name__ == '__main__':

    extract_ashare_1min_daily(today_str)

import csv

import akshare as ak
import numpy as np
import pandas as pd
import psycopg2
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import PostgresClient
from Utils.utils import convert_to_datetime
from config.conf import today_int, today_str
import io


# data_dir = r'E:\\QMT\\GJ_QMT\\datadir'

def insert_stock_eod_price_history(df: pd.DataFrame, table_name: str):
    client = PostgresClient()
    df.to_sql(table_name, con=client.engine, index=False, if_exists='append')


def extract_stock_eod_price_history(start_date, end_date):
    start_time = datetime.datetime.now()
    ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
    for ticker in tqdm(ashare_list):
        try:
            # 获取不复权历史数据
            price_ori = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1d', start_time=start_date,
                                              end_time=end_date, count=-1,
                                              dividend_type='none', fill_data=True, data_dir=xtdata.data_dir)
            price_ori_df = pd.DataFrame(price_ori[ticker]).reset_index()
            price_ori_df['ticker'] = ticker
            price_ori_df = price_ori_df.rename(
                columns={'index': 'datetime', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                         'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
            # 获取后复权历史数据
            price_adj = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1d', start_time=start_date,
                                              end_time=end_date, count=-1,
                                              dividend_type='back', fill_data=True, data_dir=xtdata.data_dir)
            price_adj_df = pd.DataFrame(price_adj[ticker]).reset_index()
            price_adj_df['ticker'] = ticker
            price_adj_df = price_adj_df.rename(
                columns={'index': 'datetime', 'open': 'open_adj', 'high': 'high_adj', 'low': 'low_adj',
                         'close': 'close_adj', 'preClose': 'pre_close_adj'})
            price_adj_df = price_adj_df[
                ['datetime', 'ticker', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'pre_close_adj']]
            price_df = pd.merge(price_adj_df, price_ori_df, on=['datetime', 'ticker'])

            del price_df['time']
            price_df['datetime'] = pd.to_datetime(price_df['datetime'], format='%Y%m%d')
            price_df['datetime'] = price_df['datetime'].dt.strftime('%Y-%m-%d')
            # 插入数据库
            insert_stock_eod_price_history(price_df, table_name='ashare_eod_prices')
        except Exception as err:
            logger_datacube.error(f'[ERROR]{ticker} :{err}')
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'Successfully inserted stock eod prices,{start_date}---{end_date},cost: {end_time - start_time}')


if __name__ == '__main__':
    extract_stock_eod_price_history('20100101', '20240315')

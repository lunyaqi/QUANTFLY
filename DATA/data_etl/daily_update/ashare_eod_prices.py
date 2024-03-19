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
def psql_insert_copy(table, conn, keys, data_iter):  # mehod
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def insert_stock_eod_price_daily(df: pd.DataFrame, table_name: str):
    client = PostgresClient()
    df.to_sql(table_name, con=client.engine, index=False, if_exists='append')


def extract_stock_eod_price_daily(date):
    # 仅支持获取一天数据
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        price_ori = xtdata.get_market_data(field_list=[], stock_list=ashare_list, period='1d', start_time=date,
                                           end_time=date, count=-1, dividend_type='none', fill_data=True)
        price_ori_dfs = [pd.DataFrame(value) for value in price_ori.values()]
        price_ori_df = pd.concat(price_ori_dfs, axis=1)
        if len(price_ori_df.columns) <= 1:
            raise ValueError(f'{date}数据未更新!')
        price_ori_df.columns = price_ori.keys()
        price_ori_df = price_ori_df.reset_index()
        price_ori_df = price_ori_df.rename(
            columns={'index': 'ticker', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                     'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
        price_ori_df['datetime'] = date
        # 获取后复权历史数据
        price_adj = xtdata.get_market_data(field_list=[], stock_list=ashare_list, period='1d', start_time=date,
                                          end_time=date, count=-1,dividend_type='back', fill_data=True)
        price_adj_dfs = [pd.DataFrame(value) for value in price_adj.values()]
        price_adj_df = pd.concat(price_adj_dfs, axis=1)
        price_adj_df.columns = price_adj.keys()
        price_adj_df = price_adj_df.reset_index()
        price_adj_df = price_adj_df.rename(
            columns={'index': 'ticker', 'open': 'open_adj', 'high': 'high_adj', 'low': 'low_adj', 'close': 'close_adj',
                     'preClose': 'pre_close_adj'})
        price_adj_df['datetime'] = date
        price_adj_df = price_adj_df[
            ['datetime', 'ticker', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'pre_close_adj']]
        price_df = pd.merge(price_adj_df, price_ori_df, on=['datetime', 'ticker'])
        del price_df['time']
        price_df['datetime'] = pd.to_datetime(price_df['datetime'], format='%Y%m%d')
        price_df['datetime'] = price_df['datetime'].dt.strftime('%Y-%m-%d')
        # 插入数据库
        insert_stock_eod_price_daily(price_df, table_name='ashare_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'Successfully inserted {date} data into  ashare_eod_prices, lens= {len(price_df)},cost: {end_time - start_time}')
    except Exception as err:
        logger_datacube.error(f'[ERROR] :{err}')


if __name__ == '__main__':
    extract_stock_eod_price_daily('20240318')

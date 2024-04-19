import datetime

import pandas as pd
from tqdm import tqdm
from xtquant import xtdata

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube


def extract_stock_1min_price_history(start_date, end_date):
    start_time = datetime.datetime.now()
    ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
    for ticker in tqdm(ashare_list):
        try:
            # 获取不复权历史数据
            price_ori = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1m', start_time=start_date,
                                              end_time=end_date, count=-1,
                                              dividend_type='none', fill_data=True, data_dir=xtdata.data_dir)
            price_ori_df = pd.DataFrame(price_ori[ticker]).reset_index()
            price_ori_df['ticker'] = ticker
            price_ori_df = price_ori_df.rename(
                columns={'index': 'timestamp', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                         'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
            # 获取后复权历史数据
            price_adj = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1m', start_time=start_date,
                                              end_time=end_date, count=-1,
                                              dividend_type='back', fill_data=True, data_dir=xtdata.data_dir)
            price_adj_df = pd.DataFrame(price_adj[ticker]).reset_index()
            price_adj_df['ticker'] = ticker
            price_adj_df = price_adj_df.rename(
                columns={'index': 'timestamp', 'open': 'open_adj', 'high': 'high_adj', 'low': 'low_adj',
                         'close': 'close_adj', 'preClose': 'pre_close_adj'})
            price_adj_df = price_adj_df[
                ['timestamp', 'ticker', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'pre_close_adj']]
            price_df = pd.merge(price_adj_df, price_ori_df, on=['timestamp', 'ticker'])

            del price_df['time']
            price_df['datetime'] = pd.to_datetime(price_df['timestamp'].astype(str).str[:8], format='%Y%m%d')
            price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])
            # 插入数据库
            insert_df_to_postgres(price_df, table_name='ashare_1min')
        except Exception as err:
            logger_datacube.error(f'[HISTORY] ashare_1min {ticker} :{err}')
            continue
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully insert ashare_1min data,{start_date}---{end_date},cost time: {end_time - start_time}')


if __name__ == '__main__':
    # 从20150101 - 20240315 每个ticker有最多539117条数据
    extract_stock_1min_price_history('20150101', '20240315')

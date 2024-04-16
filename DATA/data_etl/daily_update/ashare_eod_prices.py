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
from Utils.utils import convert_to_datetime, convert_datetime_column_format,convert_ticker_to_sina_format
from config.conf import today_int, today_str
import io
import concurrent.futures

from multiprocessing.dummy import Pool as ThreadPool


def _calculate_1min_indicators(df, start_time, end_time):
    """
    按日期分组计算指标

    参数：
    data (DataFrame): 包含时间序列数据的DataFrame，必须包含'datetime'列
    start_time (str): 开始时间，格式为'HH:MM'，例如'09:30'
    end_time (str): 结束时间，格式为'HH:MM'，例如'10:00'

    返回：
    DataFrame: 包含按日期分组计算的指标，包括累计成交额、最高价、最低价和VWAP
    """
    df['timestamp_column'] = pd.to_datetime(df['timestamp'])
    # 将日期设置为索引
    df.set_index('timestamp_column', inplace=True)
    # 筛选出每天大于9:30且小于10:00的数据
    filtered_data = df.between_time(start_time, end_time)

    # 按日期分组计算指标
    filtered_data.loc[:, 'typical_price'] = (filtered_data['close'] + filtered_data['low'] + filtered_data['high']) / 3
    filtered_data.loc[:, 'price_volume'] = filtered_data['typical_price'] * filtered_data['volume']
    cum_volume_name = f"{start_time.replace(':', '')}_{end_time.replace(':', '')}_cum_volume"
    cum_amount_name = f"{start_time.replace(':', '')}_{end_time.replace(':', '')}_cum_amount"
    high_name = f"{start_time.replace(':', '')}_{end_time.replace(':', '')}_high"
    low_name = f"{start_time.replace(':', '')}_{end_time.replace(':', '')}_low"
    vwap_name = f"{start_time.replace(':', '')}_{end_time.replace(':', '')}_vwap"

    grouped_data = filtered_data.groupby(pd.Grouper(freq='D')).agg(
        **{
            cum_volume_name: ('volume', 'sum'),
            cum_amount_name: ('amount', 'sum'),
            high_name: ('high', 'max'),
            low_name: ('low', 'min'),
            vwap_name: ('price_volume', 'sum'),

        }
    ).reset_index()
    grouped_data[vwap_name] = round(grouped_data[vwap_name] / grouped_data[cum_volume_name], 2)

    grouped_data = convert_datetime_column_format(grouped_data, column_name='timestamp_column')
    if start_time == '09:30' and end_time == '15:30':
        grouped_data = grouped_data[['datetime', 'vwap']]
    return grouped_data


def _get_1min_stock_price(ticker_list, date):
    '''
    获取1分钟数据
    :param ticker_list:
    :param date:
    :return:
    '''
    price_ori = xtdata.get_local_data(field_list=[], stock_list=ticker_list, period='1m', start_time=date,
                                      end_time=date, count=-1,
                                      dividend_type='none', fill_data=True, data_dir=xtdata.data_dir)
    price_ori_dfs = [value.assign(ticker=key) for key, value in price_ori.items()]
    price_1min_df = pd.concat(price_ori_dfs, axis=0)
    if len(price_1min_df.columns) <= 1:
        raise ValueError(f'{date}数据未更新!')
    price_1min_df = price_1min_df.reset_index()
    price_1min_df = price_1min_df.rename(
        columns={ 'index': 'timestamp', 'preClose': 'pre_close',
                 'settelementPrice': 'settlement_price',
                 'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
    price_1min_df['datetime'] = pd.to_datetime(price_1min_df['timestamp'].astype(str).str[:8], format='%Y%m%d')

    return price_1min_df


i = 1


def _get_turnover_data_for_ticker(ticker, date):
    '''
    获取akshare数据
    :param ticker:
    :param date:
    :return:
    '''
    global i
    symbol = convert_ticker_to_sina_format(ticker)
    try:
        akshare_price_df = ak.stock_zh_a_daily(symbol=symbol, start_date=date, end_date=date)
        akshare_price_df = akshare_price_df[['date', 'outstanding_share', 'turnover']]
        akshare_price_df = akshare_price_df.rename(columns={'outstanding_share': 'free_float_cap', 'date': 'datetime'})
        akshare_price_df = convert_datetime_column_format(akshare_price_df)
        akshare_price_df['ticker'] = ticker
        return akshare_price_df
    except Exception as err:
        logger_datacube.error(f'[ERROR] Failed to fetch data for ticker {ticker} on date {date}: {err}')
        return None


def _get_turnover_df(ticker_list, date):
    '''
    多线程获取turnover free_float_cap
    :param ticker_list:
    :param date:
    :return:
    '''
    pool = ThreadPool(16)
    result = pool.map(lambda x: _get_turnover_data_for_ticker(x, date), ticker_list)
    pool.close()
    pool.join()

    tot_df = pd.concat(result, axis=0)

    return tot_df


def _check_limit_status(df):
    '''
    检查涨跌停状态
    :param df:
    :return:
    '''
    for _, row in df.iterrows():
        if row['ticker'].startswith('300') or row['ticker'].startswith('688'):
            row['up_limit_price'] = round((row['pre_close'] + 0.0002) * 1.2, 2)
            row['down_limit_price'] = round((row['pre_close'] + 0.0002) * 0.8, 2)
        else:
            row['up_limit_price'] = round((row['pre_close'] + 0.0002) * 1.1, 2)
            row['down_limit_price'] = round((row['pre_close'] + 0.0002) * 0.9, 2)

        row['limit_status'] = 0

        if row['close'] >= row['up_limit_price']:
            row['limit_status'] = 1
        elif row['close'] <= row['down_limit_price']:
            row['limit_status'] = -1
    return df


def _calculate_and_merge_multiple_times(price_df, price_1min_df, time_intervals):
    '''
    计算并合并多个时间段
    :param price_df:
    :param price_1min_df:
    :param time_intervals:
    :return:
    '''
    for start_time, end_time in time_intervals:
        tmp_indicator_df = _calculate_1min_indicators(price_1min_df, start_time, end_time)
        price_df = pd.merge(price_df, tmp_indicator_df, on=['datetime'], how='left')
    return price_df


def _get_divid_factors(ticker_list, date):
    '''
    获取除权因子数据 hfq_factor
    :param ticker_list:
    :param date:
    :return:
    '''
    df_list = []
    for ticker in ticker_list:
        hfq_factor_df = xtdata.get_divid_factors(ticker, start_time='', end_time='').reset_index()
        if hfq_factor_df.empty:
            hfq_factor_df = pd.DataFrame({'index': [date], 'dr': [1.0]})
        elif date not in hfq_factor_df['index'].values:
            last_dr_value = hfq_factor_df['dr'].iloc[-1]
            new_row = pd.DataFrame({'index': [date], 'dr': [last_dr_value]})
            hfq_factor_df = hfq_factor_df[['index', 'dr']]
            hfq_factor_df = hfq_factor_df._append(new_row)
        hfq_factor_df = hfq_factor_df[['index', 'dr']]
        hfq_factor_df['ticker'] = ticker
        df_list.append(hfq_factor_df)

    tot_df = pd.concat(df_list, axis=0)
    tot_df.rename(columns={'index': 'datetime', 'dr': 'hfq_factor'}, inplace=True)
    tot_df = convert_datetime_column_format(tot_df)
    return tot_df


def extract_stock_eod_price_daily(date):
    '''
    获取每日数据
    :param date:
    :return:
    '''
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
                                           end_time=date, count=-1, dividend_type='back', fill_data=True)
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
        price_df = convert_datetime_column_format(price_df)
        hfq_factor_df = _get_divid_factors(ashare_list, date)
        price_df = pd.merge(price_df, hfq_factor_df, on=['datetime', 'ticker'], how='left')

        # turnover,free_float_cap
        turnover_df = _get_turnover_df(ashare_list, date)
        price_df = pd.merge(price_df, turnover_df, on=['datetime', 'ticker'], how='left')

        price_df['change'] = price_df['close'] - price_df['pre_close']
        price_df['pct_change'] = round(price_df['change'] / price_df['pre_close'] * 100, 2)
        price_df['avg_price'] = round(price_df['amount'] / (price_df['volume'] * 100), 2)
        price_df['amplitude_close'] = round((price_df['high'] - price_df['pre_close']) / price_df['pre_close'] * 100, 2)
        price_df['amplitude_low'] = round((price_df['high'] - price_df['low']) / price_df['low'] * 100, 2)
        price_df['free_float_marketcap'] = price_df['close'] * price_df['free_float_cap']
        # 涨停状态
        price_df = _check_limit_status(price_df)
        # 分钟数据
        price_1min_df = _get_1min_stock_price(ashare_list, date)
        time_intervals = [('09:30', '15:00'), ('09:30', '11:30'), ('13:00', '15:00'),
                          ('09:30', '10:30'), ('10:30', '11:30'), ('11:00', '13:30'),
                          ('13:00', '14:00'), ('14:00', '15:00'), ('09:30', '10:00'),
                          ('14:30', '15:00'), ('11:15', '13:15'), ('09:30', '09:45'),
                          ('09:45', '10:00'), ('09:30', '09:35'), ('13:00', '13:05'),
                          ('14:55', '15:00')]
        price_df = _calculate_and_merge_multiple_times(price_df, price_1min_df, time_intervals)

        # 插入数据库
        insert_df_to_postgres(price_df, table_name='ashare_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully inserted ashare_eod_prices, {date},cost: {end_time - start_time}, lens= {len(price_df)}')
    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_eod_prices:{err}')


if __name__ == '__main__':
    # 本程序是为了日度更新ashare_eod_prices表
    extract_stock_eod_price_daily('20240318')

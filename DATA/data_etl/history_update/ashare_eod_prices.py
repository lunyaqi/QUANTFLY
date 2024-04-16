import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
import akshare as ak
from Utils.logger import logger_datacube
from Utils.Database_connector import insert_df_to_postgres
from Utils.utils import convert_ticker_to_sina_format, convert_datetime_column_format

'''
A股股票日行情
行情来源:
    xtdata
    turnover,free_float_cap
    数据来源 akshare 新浪财经
'''


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
    filtered_data['typical_price'] = (filtered_data['close'] + filtered_data['low'] + filtered_data['high']) / 3
    filtered_data['price_volume'] = filtered_data['typical_price'] * filtered_data['volume']
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


def _get_1min_stock_price(ticker, start_date, end_date):
    '''
    获取1分钟数据
    :param ticker:
    :param start_date:
    :param end_date:
    :return:
    '''
    price_ori = xtdata.get_local_data(field_list=[], stock_list=[ticker], period='1m', start_time=start_date,
                                      end_time=end_date, count=-1,
                                      dividend_type='none', fill_data=True, data_dir=xtdata.data_dir)
    price_ori_df = pd.DataFrame(price_ori[ticker]).reset_index()
    price_ori_df['ticker'] = ticker
    price_ori_df = price_ori_df.rename(
        columns={'index': 'timestamp', 'preClose': 'pre_close', 'settelementPrice': 'settlement_price',
                 'openInterest': 'open_interest', 'suspendFlag': 'suspend_flag'})
    price_ori_df['datetime'] = pd.to_datetime(price_ori_df['timestamp'].astype(str).str[:8], format='%Y%m%d')

    return price_ori_df


def _check_limit_status(df, ticker):
    '''
    检查涨跌停状态
    :param df:
    :param ticker:
    :return:
    '''
    if ticker.startswith('300') or ticker.startswith('688'):
        df['up_limit_price'] = df['pre_close'].apply(lambda x: round((x + 0.0002) * 1.2, 2))
        df['down_limit_price'] = df['pre_close'].apply(lambda x: round((x + 0.0002) * 0.8, 2))
    else:
        df['up_limit_price'] = df['pre_close'].apply(lambda x: round((x + 0.0002) * 1.1, 2))
        df['down_limit_price'] = df['pre_close'].apply(lambda x: round((x + 0.0002) * 0.9, 2))

    df['limit_status'] = 0

    for index, row in df.iterrows():
        if row['close'] >= row['up_limit_price']:
            df.at[index, 'limit_status'] = 1
        elif row['close'] <= row['down_limit_price']:
            df.at[index, 'limit_status'] = -1
    return df


def _calculate_and_merge_multiple_times(price_df, price_1min_df, time_intervals):
    for start_time, end_time in time_intervals:
        tmp_indicator_df = _calculate_1min_indicators(price_1min_df, start_time, end_time)
        price_df = pd.merge(price_df, tmp_indicator_df, on=['datetime'], how='left')
    return price_df


def _get_turnover_rate(ticker, start_date, end_date):
    '''
    获取turnover,free_float_cap
    数据来源 akshare 新浪财经
    :param ticker:
    :param start_date:
    :param end_date:
    :return:
    '''
    symbol = convert_ticker_to_sina_format(ticker)
    akshare_price_df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date)
    akshare_price_df = akshare_price_df[['date', 'outstanding_share', 'turnover']]
    akshare_price_df = akshare_price_df.rename(
        columns={'outstanding_share': 'free_float_cap', 'date': 'datetime'})
    akshare_price_df = convert_datetime_column_format(akshare_price_df)
    return akshare_price_df


def extract_stock_eod_price_history(start_date, end_date):
    '''
    Extract stock eod price history from start_date to end_date
    :param start_date:
    :param end_date:
    :return:
    '''
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
            price_df = convert_datetime_column_format(price_df)

            turnover_df = _get_turnover_rate(ticker, start_date, end_date)

            price_df = pd.merge(price_df, turnover_df, on=['datetime'], how='left')
            # 获取后复权因子，全部数据
            hfq_factor_df = xtdata.get_divid_factors(ticker, start_time='', end_time='').reset_index()
            hfq_factor_df = hfq_factor_df[['index', 'dr']]
            hfq_factor_df = hfq_factor_df.rename(columns={'index': 'datetime', 'dr': 'hfq_factor'})
            hfq_factor_df = convert_datetime_column_format(hfq_factor_df)
            price_df = pd.merge(price_df, hfq_factor_df, on=['datetime'], how='left')
            price_df['hfq_factor'] = price_df['hfq_factor'].ffill()
            price_df['hfq_factor'] = price_df['hfq_factor'].fillna(1.0)

            price_df['change'] = price_df['close'] - price_df['pre_close']
            price_df['pct_change'] = round(price_df['change'] / price_df['pre_close'] * 100, 2)
            price_df['avg_price'] = round(price_df['amount'] / (price_df['volume'] * 100), 2)
            price_df['amplitude_close'] = round(
                (price_df['high'] - price_df['pre_close']) / price_df['pre_close'] * 100, 2)
            price_df['amplitude_low'] = round((price_df['high'] - price_df['low']) / price_df['low'] * 100, 2)
            price_df['free_float_marketcap'] = price_df['close'] * price_df['free_float_cap']
            # 涨停状态
            price_df = _check_limit_status(price_df, ticker)
            # 分钟数据
            price_1min_df = _get_1min_stock_price(ticker, start_date, end_date)
            time_intervals = [('09:30', '15:00'), ('09:30', '11:30'), ('13:00', '15:00'),
                              ('09:30', '10:30'), ('10:30', '11:30'), ('11:00', '13:30'),
                              ('13:00', '14:00'), ('14:00', '15:00'), ('09:30', '10:00'),
                              ('14:30', '15:00'), ('11:15', '13:15'), ('09:30', '09:45'),
                              ('09:45', '10:00'), ('09:30', '09:35'), ('13:00', '13:05'),
                              ('14:55', '15:00')]
            price_df = _calculate_and_merge_multiple_times(price_df, price_1min_df, time_intervals)

            # 插入数据库
            insert_df_to_postgres(price_df, table_name='ashare_eod_prices')
        except Exception as err:
            logger_datacube.error(f'[HISTORY] ashare_eod_prices {ticker} :{err}')
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully inserted ashare_eod_prices,{start_date}-{end_date},cost time: {end_time - start_time}')


if __name__ == '__main__':
    extract_stock_eod_price_history('20100101', '20240315')

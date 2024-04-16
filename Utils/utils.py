import datetime
import math
import os
import statsmodels.api as sm
import akshare as ak
import h5py
import pandas as pd
import numpy as np
import pymysql
from Utils import logger
import time


def add_exchange_suffix(ticker):
    if ticker.startswith('60') or ticker.startswith('90') or ticker.startswith('68') :
        return ticker + '.SH'
    elif ticker.startswith('0') or ticker.startswith('3'):
        return ticker + '.SZ'
    elif ticker.startswith('5'):
        return ticker + '.SH'
    elif ticker.startswith('8'):
        return ticker + '.BJ'
    else:
        return ticker
def convert_datetime_column_format(df, column_name=None):
    if column_name:
        df = df.rename(columns={column_name: 'datetime'})

    # Check the data type of the 'datetime' column
    if df['datetime'].dtypes != 'datetime64[ns]':
        try:
            # Try to convert the 'datetime' column to datetime format
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d')
        except ValueError:
            # If the above conversion fails, try another format
            try:
                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
            except ValueError:
                # If all conversions fail, raise an error
                raise ValueError("The 'datetime' column is not in a recognized date format.")

    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')
    return df


def convert_ticker_to_sina_format(stock_code):
    """
    Convert stock code like "000001.SH" to "sh000001".

    Parameters:
    stock_code (str): The original stock code in the format "XXXYYY.ZZ" where XXX is the stock number,
                        YYY is the exchange abbreviation, and ZZ is the extension.

    Returns:
    str: The converted stock code in the format "zzxxx".
    """
    # Split the original code by '.' to separate the exchange abbreviation and the stock number
    parts = stock_code.split('.')
    if len(parts) != 2:
        raise ValueError("Stock code format is incorrect. Expected format is 'XXXYYY.ZZ'.")

    # Extract the stock number and exchange abbreviation
    stock_number = parts[0]
    exchange = parts[1]

    # Check if the exchange abbreviation is valid
    if len(exchange) != 2 or not exchange.isalpha():
        raise ValueError("Exchange abbreviation must be two alphabetic characters.")

    # Construct the new code by prefixing the stock number with the exchange code
    new_code = exchange.lower() + stock_number
    return new_code


def conv_time(ct):
    '''
    conv_time(1476374400000) --> '20161014000000.000'
    '''
    local_time = time.localtime(ct / 1000)
    data_head = time.strftime('%Y%m%d%H%M%S', local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = '%s.%03d' % (data_head, data_secs)
    return time_stamp


def _convert_to_int(query_date):
    # for one element, not list
    if isinstance(query_date, int) or isinstance(query_date, np.int64) or isinstance(query_date, np.int32):
        return query_date
    elif isinstance(query_date, str):
        # unicode to str
        query_date = query_date.encode('utf-8').decode('utf-8')
        return int(pd.Timestamp(query_date).strftime('%Y%m%d'))
    elif isinstance(query_date, datetime.datetime):
        return int(query_date.strftime('%Y%m%d'))
    else:
        print('Unknown date type.')


def convert_to_int(date):
    if (isinstance(date, str)) or isinstance(date, int) or \
            isinstance(date, datetime.datetime) or isinstance(date, np.int64) or isinstance(date, np.int32):
        return _convert_to_int(date)
    else:
        return np.array(list(map(_convert_to_int, date)))


def replace(base, from_value, to_value):
    for f, t in zip(from_value, to_value):
        base[base == f] = t
    return base


def date_range(beginDate, endDate):
    '''获取指定日期范围所有日期列表'''
    dates = []
    format_date = "%Y%m%d"
    dt = datetime.datetime.strptime(beginDate, format_date)
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime(format_date)
    return dates


def format_date(x):
    x = str(x)
    x = x.replace(':', '')
    x = x.replace(' ', '')
    x = x.replace('-', '')
    x = x.replace('.', '')
    return x


def _tools_read_h5(daily_h5_path):
    """
	读取单个h5文件
	:param daily_h5_path: h5文件
	:return: 返回数据
	"""
    if not os.path.exists(daily_h5_path):
        raise Exception(f"not found h5 file:{daily_h5_path}")
    else:
        with h5py.File(daily_h5_path, 'r') as daily_h5:
            # return all data
            ret_dict = {}
            group_key = list(daily_h5.keys())
            for group_name in group_key:
                ret_dict[group_name] = {}
                ret_dict[group_name] = daily_h5[group_name][:]

            return ret_dict


def read_sql_file(sqls_path, sql_file_name: str) -> str:
    sql_file = os.path.join(sqls_path, f'{sql_file_name}.sql')
    with open(sql_file, 'r') as f:
        sql = f.read()
    return sql


def get_trading_days(s_start_date: object, s_end_date: object) -> object:
    s_start_date = convert_to_datetime(s_start_date)
    s_end_date = convert_to_datetime(s_end_date)
    trade_date_df = ak.tool_trade_date_hist_sina()
    trade_date_df['trade_date'] = convert_to_datetime(trade_date_df['trade_date'])
    trade_date_df = trade_date_df[
        (trade_date_df['trade_date'] >= s_start_date) & (trade_date_df['trade_date'] <= s_end_date)]
    return np.array(trade_date_df['trade_date'])


def _move2_next_busday(Date):
    Date = _convert_to_datetime(Date)
    tmpdate = Date + pd.Timedelta(days=1)
    while not is_trading_day(tmpdate):
        tmpdate = tmpdate + pd.Timedelta(days=1)
    return _convert_to_int(tmpdate)


def _move2_last_busday(Date):
    Date = _convert_to_datetime(Date)
    tmpdate = Date - pd.Timedelta(days=1)
    while not is_trading_day(tmpdate):
        tmpdate = tmpdate - pd.Timedelta(days=1)
    return _convert_to_int(tmpdate)


def move_x_calendar_day(date, days):
    date = convert_to_int(date)
    date = str(date).encode('utf-8').decode('utf-8')
    date = pd.Timestamp(date) + pd.Timedelta(days=days)
    return convert_to_int(date)


def move2_last_busday(Date):
    if (isinstance(Date, str)) or isinstance(Date, int) or \
            isinstance(Date, datetime.datetime) or isinstance(Date, np.int64) or isinstance(Date, np.int32):
        return _move2_last_busday(Date)
    else:
        return np.array(list(map(_move2_last_busday, Date)))


def move2_next_busday(Date):
    if (isinstance(Date, str)) or isinstance(Date, int) or \
            isinstance(Date, datetime.datetime) or isinstance(Date, np.int64) or isinstance(Date, np.int32):
        return _move2_next_busday(Date)
    else:
        return np.array(list(map(_move2_next_busday, Date)))


def split_by_n(list_d: list, n: int = 5) -> list:
    """
	分割list
	:param list_d:
	:param n:
	:return:
	"""
    return_list = list()
    for i in range(0, len(list_d), n):
        sub_list = list_d[i:i + n]
        return_list.append(sub_list)
    return return_list


def _convert_to_datetime(query_date: str or datetime.date or datetime.datetime or int):
    """
	standardize input query_date to datetime.datetime
	:param query_date: query date
	:return: datetime.datetime
	"""
    if isinstance(query_date, datetime.date) and not isinstance(query_date, datetime.datetime):
        return datetime.datetime.fromordinal(query_date.toordinal())
    elif isinstance(query_date, str):
        query_date = query_date.encode('utf-8').decode('utf-8')
        return pd.Timestamp(query_date).to_pydatetime()
    elif isinstance(query_date, datetime.datetime):
        return query_date
    else:
        if len(str(query_date)) == 8:
            return datetime.datetime.strptime(str(query_date), '%Y%m%d')
        else:
            return datetime.datetime.strptime(str(query_date), '%Y%m%d%H%M%S')


def is_trading_day(date):
    if (isinstance(date, str)) or isinstance(date, int) or \
            isinstance(date, datetime.datetime) or isinstance(date, np.int64) or isinstance(date, np.int32):
        date = _convert_to_int(date)
        flag = True
    else:
        date = list(map(_convert_to_int, date))
        flag = False

    if flag:
        return _is_trading_date_for_int(date)
    else:
        return np.array(list(map(_is_trading_date_for_int, date)))


def _is_trading_date_for_int(dt, check=True):
    # 这边不能写死
    date_all = pd.read_csv('..\local_mktTradeDays.csv')
    date_list = list(date_all['date'])
    if dt in date_list:
        return True
    else:
        return False


def get_all_date(sdate, edate):
    AllDate = np.array(pd.date_range(str(sdate), str(edate)).strftime('%Y%m%d').astype(int))
    # print("all date from {0} to {1}, {2} records in total.".format(self.sdate, self.edate, len(AllDate)))
    return AllDate


def get_date_list(sdate, edate, freq='d', nthday=1, isbusday=0):
    # format of sdate and edate: '2020-12-1'
    # freq could be 'd', 'busday', 'w', 'm', 'q', 'y'
    # nthday means nth busday/d of w/m/q/y, -1 is end of m/w/q/y
    # isbusday: 1 yes, 0 no
    assert isbusday in [0, 1], "isbusday must be 0 or 1"
    assert nthday >= 1 or nthday == -1, "pls reset nthday"
    freq = freq.lower()

    if freq == 'busday':  # business day
        FinalDate = get_trading_days(sdate, edate)

    elif freq == 'd':
        if isbusday == 1:
            FinalDate = get_trading_days(sdate, edate)
        else:
            FinalDate = get_all_date(sdate, edate)

    else:
        if isbusday == 1:
            ## bug if you want to choose Friday, but there are only 4 busdays in that week!!!!!
            Busday = pd.date_range(sdate, edate, freq='D')
            Busday = Busday[is_trading_day(Busday)]
            Busday = pd.DataFrame([Busday, Busday.to_period(freq=freq)], index=['raw', 'period']).T
            if nthday > 0:
                FinalDate = Busday.groupby('period').last()
                Nth = Busday.groupby('period').nth(nthday - 1)
                FinalDate.loc[Nth.index, :] = Nth
                if FinalDate.index[-1].strftime('%Y%m%d') != Nth.index[-1].strftime('%Y%m%d'):
                    FinalDate = FinalDate.iloc[:-1, :]
            elif nthday == -1:
                FinalDate = Busday.groupby('period').last()
            FinalDate['raw'] = [int(x.strftime('%Y%m%d')) for x in FinalDate['raw']]
            FinalDate = np.array(FinalDate['raw'])

        elif isbusday == 0:
            if freq == 'w':  # weekly date
                if nthday > 0:
                    Map = {1: 'W-MON', 2: 'W-TUE', 3: 'W-WED', 4: 'W-THU', 5: 'W-FRI', 6: 'W-SAT', 7: 'W-SUN'}
                    FinalDate = pd.date_range(start=sdate, end=edate, freq=Map[nthday]). \
                        strftime('%Y%m%d').astype(int)
                elif nthday == -1:
                    FinalDate = pd.date_range(start=sdate, end=edate, freq='W-SUN'). \
                        strftime('%Y%m%d').astype(int)  # Sunday is end of week

            elif freq == 'm':  # monthly date
                if nthday > 0:
                    FinalDate = (pd.date_range(start=pd.Timestamp(sdate) - pd.Timedelta(days=30), end=edate,
                                               freq='MS') + \
                                 pd.Timedelta(days=(nthday - 1))).strftime('%Y%m%d').astype(int)
                elif nthday == -1:
                    FinalDate = pd.date_range(start=sdate, end=edate, freq='M'). \
                        strftime('%Y%m%d').astype(int)

            elif freq == 'q':  # quarterly date
                if nthday > 0:
                    FinalDate = pd.date_range(start=pd.Timestamp(sdate) - pd.Timedelta(days=90), end=edate,
                                              freq='MS')
                    FinalDate = (FinalDate[np.mod(FinalDate.month, 3) == 1] + pd.Timedelta(days=(nthday - 1))). \
                        strftime('%Y%m%d').astype(int)
                elif nthday == -1:
                    FinalDate = pd.date_range(start=sdate, end=edate, freq='Q'). \
                        strftime('%Y%m%d').astype(int)

            elif freq == 'y':  # yearly date
                if nthday > 0:
                    FinalDate = pd.date_range(start=pd.Timestamp(sdate) - pd.Timedelta(days=365), end=edate,
                                              freq='MS')
                    FinalDate = (FinalDate[FinalDate.month == 1] + pd.Timedelta(days=(nthday - 1))). \
                        strftime('%Y%m%d').astype(int)
                elif nthday == -1:
                    FinalDate = pd.date_range(start=sdate, end=edate, freq='Q')
                    FinalDate = FinalDate[FinalDate.month == 12].strftime('%Y%m%d').astype(int)

    FinalDate = FinalDate[
        (FinalDate >= int(pd.Timestamp(sdate).strftime('%Y%m%d'))) & \
        (FinalDate <= int(pd.Timestamp(edate).strftime('%Y%m%d')))
        ]
    return FinalDate


def convert_to_datetime(date):
    if (isinstance(date, str)) or isinstance(date, int) or \
            isinstance(date, datetime.datetime) or isinstance(date, np.int64) or isinstance(date, np.int32):
        return _convert_to_datetime(date)
    else:
        return np.array(list(map(_convert_to_datetime, date)))


def traversal_files(path):
    files = []
    dirs = []
    for item in os.scandir(path):
        if item.is_dir():
            dirs.append(item.path)
        elif item.is_file():
            files.append(item.path)
    return files


'''数据清洗函数工具'''
'''
mad中位数去极值法
series:待处理数据，Series
n：几个单位的偏离值
'''


def filter_extreme_MAD(series_pre, n):
    median = series_pre.quantile(0.5)
    new_median = ((series_pre - median).abs()).quantile(0.50)
    max_range = median + n * new_median
    min_range = median - n * new_median
    return np.clip(series_pre, min_range, max_range)


'''
方差去极值
series_pre:待处理数据，数据类型Series
std:为几倍的标准差，
have_negative 为布尔值，是否包括负值
'''


def filter_extreme_std(series_pre, n=3, have_negative=True):
    series_pre_copy = series_pre.dropna().copy()
    if have_negative == False:
        series_pre_copy = series_pre_copy[series_pre_copy >= 0]
    else:
        pass
    edge_up = series_pre_copy.mean() + n * series_pre_copy.std()
    edge_low = series_pre_copy.mean() - n * series_pre_copy.std()
    series_pre_copy[series_pre_copy > edge_up] = edge_up
    series_pre_copy[series_pre_copy < edge_low] = edge_low
    return series_pre_copy


'''
标准化函数：
series_pre:待处理数据，数据类型Series
ty：标准化类型:1 MinMax,2 Standard,3 maxabs
'''


def standardize(series_pre, ty=2):
    series_pre_copy = series_pre.dropna().copy()
    if int(ty) == 1:
        re = (series_pre_copy - series_pre_copy.min()) / (series_pre_copy.max() - series_pre_copy.min())
    elif ty == 2:
        std = series_pre_copy.std()
        if std == 0:
            std = 1
        re = (series_pre_copy - series_pre_copy.mean()) / std
    elif ty == 3:
        re = series_pre_copy / (10 ** np.ceil(np.log10(series_pre_copy.abs().max())))
    return re


def standardize_df(factor_data, ty=2):
    result = pd.DataFrame()
    for factor in factor_data.columns:
        result[factor] = standardize(factor_data[factor], ty=2)
    return result


'''
缺失值处理，一般情况应该去除drop（how = ‘any’）,但A股数据量本来缺少，最终还是决定以均值替代
若是行业中因子值为空，则以所有行业均值代替
某只股票因子值为空，用行业平均值代替，
依然会有nan，则用所有股票平均值代替
'''


# def replace_nan_indu(self, factor_data, stockList, industry_code, date):
# 	i_Constituent_Stocks = {}
# 	if isinstance(factor_data, pd.DataFrame):
# 		data_temp = pd.DataFrame(index=industry_code, columns=factor_data.columns)
# 		for i in industry_code:
# 			temp = get_industry_stocks(i, date)
# 			i_Constituent_Stocks[i] = list(set(temp).intersection(set(stockList)))
# 			data_temp.loc[i] = np.mean(factor_data.loc[i_Constituent_Stocks[i], :])
# 		for factor in data_temp.columns:
# 			# 行业缺失值用所有行业平均值代替
# 			null_industry = list(data_temp.loc[pd.isnull(data_temp[factor]), factor].keys())
# 			for i in null_industry:
# 				data_temp.loc[i, factor] = np.mean(data_temp[factor])
# 			null_stock = list(factor_data.loc[pd.isnull(factor_data[factor]), factor].keys())
# 			for i in null_stock:
# 				industry = get_key(i_Constituent_Stocks, i)
# 				if industry:
# 					factor_data.loc[i, factor] = data_temp.loc[industry[0], factor]
# 				else:
# 					factor_data.loc[i, factor] = np.mean(factor_data[factor])
# 	return factor_data

def fill_missing_values(df, factor_name, type=1):
    if type == 1:
        # 用每只股票各自前后10天的数据均值填充,开始日期的值用第一个有效值填充
        rolling_mean = df.groupby('Ticker')[factor_name].rolling(window=21, center=True,
                                                                 min_periods=1).mean().reset_index()
        rolling_mean[factor_name] = rolling_mean.groupby('Ticker')[factor_name].fillna(method='bfill')
        rolling_mean[factor_name] = rolling_mean.groupby('Ticker')[factor_name].fillna(method='ffill')
        df = df.sort_values(by=['Ticker', 'Date'])
        df[factor_name].fillna(rolling_mean[factor_name], inplace=True)

    elif type == 2:
        # 根据该股票前一个数据填充
        df[factor_name] = df.groupby('Ticker')[factor_name].fillna(method='ffill')
        df = df.fillna(0)

    elif type == 3:
        # 用该股票当天同行业的均值填充空值
        grouped = df.groupby(['Date', 'IndustryCode']).transform(lambda x: x.fillna(x.mean()))
        df[factor_name].fillna(grouped[factor_name], inplace=True)
    elif type == 4:
        # 插值填充
        df[factor_name] = df[factor_name].interpolate(method='linear')
        df[factor_name] = df.groupby('Ticker')[factor_name].fillna(method='bfill')
    df = df.fillna(0)
    df = df.set_index(['Date', 'Ticker']).sort_index()
    num = df[factor_name].isna().sum()
    logger.info(f'{factor_name}:{num}')

    return df


'''
中性化函数，对需要待中性化的因子进行中性化回归，其残差即为中性化后的因子值
输入：
mkt_cap：以股票为index，市值为value的Series,
factor：以股票code为index，因子值为value的Series,
输出：
中性化后的因子值series
'''


def neutralize_df(factor_df, factor_name, mkt_cap_col=None, industry_col=None):
    factor_df = factor_df.set_index(['Date', 'Ticker'])
    result = pd.DataFrame()
    y = factor_df[factor_name]
    if mkt_cap_col is not None:
        mkt_cap = factor_df[mkt_cap_col]
        if industry_col is not None:
            dummy_industry = pd.get_dummies(factor_df[industry_col])
            x = pd.concat([mkt_cap.apply(lambda x: math.log(x)), dummy_industry], axis=1)
        else:
            x = mkt_cap.apply(lambda x: math.log(x))
    elif industry_col is not None:
        dummy_industry = pd.get_dummies(factor_df[industry_col])
        x = dummy_industry
    else:
        raise ValueError("At least one of 'mkt_cap_col' and 'industry_col' should be provided.")
    result[factor_name] = sm.OLS(y.astype(float), x.astype(float)).fit().resid
    result.reset_index(inplace=True)
    return result[factor_name]

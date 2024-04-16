import time

import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import insert_df_to_postgres
import akshare as ak

from Utils.utils import convert_to_datetime
from multiprocessing.dummy import Pool as ThreadPool


def get_indu_hist_sw_df(args):
    indu_ticker, start_date, end_date, symbol_all, name_all = args
    indu_symbol = indu_ticker[:6]
    indu_hist_sw_df = ak.index_hist_sw(symbol=indu_symbol, period="day")
    indu_hist_sw_df = indu_hist_sw_df[
        (indu_hist_sw_df['日期'] >= start_date) & (indu_hist_sw_df['日期'] <= end_date)]
    if indu_hist_sw_df.empty:
        logger_datacube.error(f'[DAILY] sw_indu_eod_prices {indu_ticker} 未获取到行情数据')
        return None
    indu_hist_sw_df['indu_name'] = name_all[symbol_all.index(indu_ticker)]
    return indu_hist_sw_df


def extract_sw_indu_eod_prices_daily(start_date, end_date):
    start_time = datetime.datetime.now()
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()

    try:
        info_dfs = [ak.sw_index_first_info(), ak.sw_index_second_info(), ak.sw_index_third_info()]
        symbol_all, name_all = [], []

        for df in info_dfs:
            symbol_all.extend(df['行业代码'].to_list())
            name_all.extend(df['行业名称'].to_list())

        with ThreadPool(8) as pool:
            dfs = pool.map(get_indu_hist_sw_df,
                           [(indu_ticker, start_date, end_date, symbol_all, name_all) for indu_ticker in symbol_all])
        dfs = [df for df in dfs if df is not None]
        sw_hist_total = pd.concat(dfs, ignore_index=True)
        sw_hist_total.columns = ['indu_code', 'datetime', 'close', 'open', 'high', 'low', 'volume', 'amount',
                                 'indu_name']
        sw_hist_total['volume'] = sw_hist_total['volume'].astype(float) * 100000000.0
        sw_hist_total['amount'] = sw_hist_total['amount'].astype(float) * 100000000.0
        sw_hist_total['datetime'] = convert_to_datetime(sw_hist_total['datetime'])
        # 插入数据库
        insert_df_to_postgres(sw_hist_total, table_name='sw_indu_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert sw_indu_eod_prices,{start_date}-{end_date},cost time: {end_time - start_time},lens={len(sw_hist_total)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] sw_indu_eod_prices:{err}')


if __name__ == '__main__':
    extract_sw_indu_eod_prices_daily(start_date='2024-03-15', end_date='2024-03-15')

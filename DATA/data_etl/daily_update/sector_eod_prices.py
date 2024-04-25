import datetime
import time

import akshare as ak
import pandas as pd

from Config.conf import today_format
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_datetime_column_format

'''
同花顺概念板块日行情
数据来源: akshare/同花顺
落地数据库:akshare/sector_eod_prices
'''


def _get_sector_eod_prices(sector_name, ticker, start_date, end_date):
    # 重试100次
    for attempt in range(10):
        try:
            stock_board_concept_hist_ths_df = ak.stock_board_concept_hist_ths(start_year="2010", symbol=sector_name)
            stock_board_concept_hist_ths_df['sector_name'] = sector_name
            stock_board_concept_hist_ths_df['ticker'] = ticker
            stock_board_concept_hist_ths_df['日期'] = stock_board_concept_hist_ths_df['日期'].astype(str)
            stock_board_concept_hist_ths_df = stock_board_concept_hist_ths_df[
                (stock_board_concept_hist_ths_df['日期'] >= start_date) & (
                        stock_board_concept_hist_ths_df['日期'] <= end_date)]
            return stock_board_concept_hist_ths_df
        except Exception as e:
            logger_datacube.error(f'[Error] 第{attempt}次尝试, {sector_name} 未获取到行情数据,进行下一次尝试...')
            # 如果不是最后一次尝试，那么等待一段时间再重试
            time.sleep(1)
            continue
    # 如果是最后一次尝试，那么记录错误并跳过这个概念板块
    logger_datacube.error(f'[Error] {sector_name} 未获取到行情数据')
    return None


def extract_sector_eod_prices_daily(start_date, end_date):
    start_time = datetime.datetime.now()
    try:
        stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()
        sector_list = stock_board_concept_name_ths_df['概念名称'].to_list()
        ticker_list = stock_board_concept_name_ths_df['代码'].to_list()
        sector_eod_prices_dfs = []

        for i in range(len(sector_list)):
            sector_name = sector_list[i]
            ticker = ticker_list[i]
            stock_board_concept_hist_ths_df = _get_sector_eod_prices(sector_name, ticker, start_date,
                                                                     end_date)
            sector_eod_prices_dfs.append(stock_board_concept_hist_ths_df)
        sector_eod_prices_df = pd.concat(sector_eod_prices_dfs, ignore_index=True)
        sector_eod_prices_df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'amount', 'sector_name',
                                        'sector_code']
        sector_eod_prices_df = convert_datetime_column_format(sector_eod_prices_df)
        # 插入数据库

        insert_df_to_postgres(sector_eod_prices_df, table_name='sector_eod_prices')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully inserted sector_eod_prices {start_date}-{end_date} data,cost: {end_time - start_time},len={len(sector_eod_prices_df)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] sector_eod_prices:{err}')
        return


if __name__ == '__main__':
    extract_sector_eod_prices_daily(start_date=today_format, end_date=today_format)

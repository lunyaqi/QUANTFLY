import datetime
import time

import akshare as ak
import pandas as pd

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_datetime_column_format, add_exchange_suffix

'''
同花顺概念板块成分股
数据来源: akshare/同花顺
落地数据库:akshare/sector_eod_prices
'''


def _get_sector_cons(sector_name, sector_symbol, open_date):
    # 重试100次
    for attempt in range(100):
        try:
            stock_board_concept_cons_ths_df = ak.stock_board_concept_cons_ths(symbol=sector_name)
            stock_board_concept_cons_ths_df = stock_board_concept_cons_ths_df[['代码']]
            stock_board_concept_cons_ths_df = stock_board_concept_cons_ths_df.rename(
                columns={'代码': 'ticker'})
            stock_board_concept_cons_ths_df['sector_name'] = sector_name
            stock_board_concept_cons_ths_df['sector_code'] = sector_symbol
            stock_board_concept_cons_ths_df['open_date'] = open_date
            return stock_board_concept_cons_ths_df
        except Exception as e:
            logger_datacube.error(f'[Error] 第{attempt}次尝试, {sector_name} 未获取到概念成分股,进行下一次尝试...')
            # 如果不是最后一次尝试，那么等待一段时间再重试
            time.sleep(5)
            continue
    # 如果是最后一次尝试，那么记录错误并跳过这个概念板块
    logger_datacube.error(f'[Error] {sector_name} 未获取到行情数据')
    return None


def extract_sector_cons_history():
    start_time = datetime.datetime.now()
    try:
        # 获取所有概念板块
        stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()
        sector_list = stock_board_concept_name_ths_df['概念名称'].to_list()
        sector_code_list = stock_board_concept_name_ths_df['代码'].to_list()
        open_date_list = stock_board_concept_name_ths_df['日期'].to_list()
        sector_cons_ths_dfs = []
        for i in range(len(sector_list)):
            sector_name = sector_list[i]
            sector_symbol = sector_code_list[i]
            open_date = datetime.date(1900, 1, 1) if not open_date_list[i] else open_date_list[i]
            try:
                sector_cons_ths_df = _get_sector_cons(sector_name, sector_symbol, open_date)
                time.sleep(1)

                sector_cons_ths_dfs.append(sector_cons_ths_df)
            except Exception as e:
                logger_datacube.error(f'Error {sector_name} 未获取到概念板块成分股数据')
                continue
        sector_cons_df = pd.concat(sector_cons_ths_dfs, ignore_index=True)
        sector_cons_df['ticker'] = sector_cons_df['ticker'].apply(add_exchange_suffix)
        sector_cons_df = convert_datetime_column_format(sector_cons_df)
        insert_df_to_postgres(sector_cons_df, table_name='sector_cons')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] sector_cons_df:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully insert sector_cons,cost: {end_time - start_time},lens={len(sector_cons_df)}')


if __name__ == '__main__':
    extract_sector_cons_history()

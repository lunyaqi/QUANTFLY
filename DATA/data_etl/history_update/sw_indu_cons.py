import datetime

import akshare as ak
import pandas as pd

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_to_datetime, add_exchange_suffix


def exstract_sw_indu_cons_history(start_date, end_date):
    start_time = datetime.datetime.now()
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()

    try:

        sw1_symbol_list = ak.sw_index_first_info()['行业代码'].to_list()
        sw2_symbol_list = ak.sw_index_second_info()['行业代码'].to_list()
        sw3_symbol_list = ak.sw_index_third_info()['行业代码'].to_list()
        sw1_name_list = ak.sw_index_first_info()['行业名称'].to_list()
        sw2_name_list = ak.sw_index_second_info()['行业名称'].to_list()
        sw3_name_list = ak.sw_index_third_info()['行业名称'].to_list()
        symbol_all = sw1_symbol_list + sw2_symbol_list + sw3_symbol_list
        name_all = sw1_name_list + sw2_name_list + sw3_name_list

        sw_indu_cons_dfs = []
        for indu_ticker in symbol_all:

            indu_symbol = indu_ticker[:6]
            indu_cons_df = ak.index_component_sw(symbol=indu_symbol)
            indu_cons_df['indu_name'] = name_all[symbol_all.index(indu_ticker)]
            indu_cons_df['indu_code'] = indu_symbol

            if indu_ticker in sw1_symbol_list:
                indu_cons_df['indu_type'] = 'SW1'
            elif indu_ticker in sw2_symbol_list:
                indu_cons_df['indu_type'] = 'SW2'
            else:
                indu_cons_df['indu_type'] = 'SW3'

            sw_indu_cons_dfs.append(indu_cons_df)

        total_sw_indu_cons = pd.concat(sw_indu_cons_dfs, ignore_index=True)
        del total_sw_indu_cons['序号']
        total_sw_indu_cons.columns = ['ticker', 'ticker_name', 'weight', 'open_date', 'indu_name', 'indu_code',
                                      'indu_type']
        del total_sw_indu_cons['ticker_name']
        total_sw_indu_cons['ticker'] = total_sw_indu_cons['ticker'].apply(add_exchange_suffix)
        total_sw_indu_cons = total_sw_indu_cons[
            (total_sw_indu_cons['open_date'] <= end_date) & (total_sw_indu_cons['open_date'] >= start_date)]
        # 先清空表
        insert_df_to_postgres(total_sw_indu_cons, table_name='sw_indu_cons')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[HISTORY] successfully inserted sw_indu_cons,cost: {end_time - start_time},lens={len(total_sw_indu_cons)}')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] sw_indu_cons:{err}')


if __name__ == '__main__':
    exstract_sw_indu_cons_history(start_date='2024-01-01', end_date='2024-04-15')

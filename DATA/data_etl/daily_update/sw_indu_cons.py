import datetime
import akshare as ak
import pandas as pd
from tqdm import tqdm

from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import convert_to_datetime, add_exchange_suffix

'''
申万行业成分股
数据来源：akshare
'''

def exstract_sw_indu_cons_daily(start_date, end_date):
    start_time = datetime.datetime.now()
    start_date = convert_to_datetime(start_date).date()
    end_date = convert_to_datetime(end_date).date()

    try:
        info_dfs = [ak.sw_index_first_info(), ak.sw_index_second_info(), ak.sw_index_third_info()]
        symbol_all, name_all = [], []
        for df in info_dfs:
            symbol_all.extend(df['行业代码'].to_list())
            name_all.extend(df['行业名称'].to_list())
        sw_indu_cons_dfs = []
        for indu_ticker in tqdm(symbol_all):
            indu_symbol = indu_ticker[:6]
            indu_cons_df = ak.index_component_sw(symbol=indu_symbol)
            indu_cons_df['indu_name'] = name_all[symbol_all.index(indu_ticker)]
            indu_cons_df['indu_code'] = indu_symbol
            sw_indu_cons_dfs.append(indu_cons_df)

        total_sw_indu_cons = pd.concat(sw_indu_cons_dfs, ignore_index=True)
        del total_sw_indu_cons['序号']
        total_sw_indu_cons.columns = ['ticker', 'ticker_name', 'weight', 'open_date', 'indu_name', 'indu_code']
        del total_sw_indu_cons['ticker_name']
        total_sw_indu_cons['ticker'] = total_sw_indu_cons['ticker'].apply(add_exchange_suffix)
        total_sw_indu_cons = total_sw_indu_cons[
            (total_sw_indu_cons['open_date'] <= end_date) & (total_sw_indu_cons['open_date'] >= start_date)]
        if total_sw_indu_cons.empty:
            logger_datacube.warning(
                f'[DAILY] sw_indu_cons:{start_date}-{end_date},no data!')

        insert_df_to_postgres(total_sw_indu_cons, table_name='sw_indu_cons')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert sw_indu_cons,{start_date}-{end_date},cost time: {end_time - start_time},lens={len(total_sw_indu_cons)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] sw_indu_cons:{err}')



if __name__ == '__main__':
    exstract_sw_indu_cons_daily(start_date='2024-01-01', end_date='2024-04-15')

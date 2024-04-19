import datetime

import akshare as ak

from Config.conf import today_str
from Utils.Database_connector import insert_df_to_postgres
from Utils.logger import logger_datacube
from Utils.utils import add_exchange_suffix, convert_datetime_column_format

'''
A股ST股票列表
数据来源：akshare
'''


def extract_ashare_st_daily(date):
    start_time = datetime.datetime.now()
    try:
        st_df = ak.stock_zh_a_st_em()
        st_df = st_df[['代码']]
        st_df['datetime'] = date
        st_df.columns = ['ticker', 'datetime']
        st_df['ticker'] = st_df['ticker'].apply(add_exchange_suffix)
        st_df = convert_datetime_column_format(st_df)
        if st_df.empty:
            logger_datacube.info(f'[Daily] ashare_st: {date} 未更新')
            return
        # 插入数据库
        insert_df_to_postgres(st_df, table_name='ashare_st')
        end_time = datetime.datetime.now()

        logger_datacube.info(
            f'[DAILY] Successfully insert  ashare_st,{date} ,cost: {end_time - start_time},lens={len(st_df)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_st:{err}')
        return

if __name__ == '__main__':
    extract_ashare_st_daily(today_str)

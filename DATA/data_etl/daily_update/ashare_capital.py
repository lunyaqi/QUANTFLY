import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import PostgresClient, insert_df_to_postgres
from config.conf import today_str


def extract_ashare_capital_daily(start_date: str, end_date: str):
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_ashare_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=['capital'], start_time=start_date, end_time=end_date,
                                                   report_type='report_time')
        capital_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['capital'])
            financial_ori_df['ticker'] = ticker
            capital_df = pd.concat([capital_df, financial_ori_df], axis=0)
        capital_df = capital_df.rename(columns={
            'freeFloatCapital':'free_float_capital',
        })
        if capital_df.empty:
            logger_datacube.info(f'[DAILY] ashare_capital:{start_date}-{end_date} :未更新')
            return
        # 插入数据库
        insert_df_to_postgres(capital_df, table_name='ashare_capital')
        end_time = datetime.datetime.now()
        logger_datacube.info(
            f'[DAILY] successfully insert ashare_capital,{start_date}-{end_date} ,cost: {end_time - start_time},lens={len(capital_df)}')

    except Exception as err:
        logger_datacube.error(f'[DAILY] ashare_capital:{err}')
        return

if __name__ == '__main__':
    extract_ashare_capital_daily(start_date=today_str,end_date=today_str)

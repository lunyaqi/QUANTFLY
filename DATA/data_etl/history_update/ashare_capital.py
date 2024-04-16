import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import insert_df_to_postgres



def extract_stock_capital_history():
    start_time = datetime.datetime.now()
    try:
        ashare_list = xtdata.get_stock_list_in_sector('沪深A股')
        financial_data = xtdata.get_financial_data(ashare_list, table_list=['capital'], start_time='', end_time='',
                                                   report_type='report_time')
        capital_df = pd.DataFrame()
        for ticker in tqdm(ashare_list):
            financial_ori_df = pd.DataFrame(financial_data[ticker]['capital'])
            financial_ori_df['ticker'] = ticker
            capital_df = pd.concat([capital_df, financial_ori_df], axis=0)
        capital_df = capital_df.rename(columns={
            'freeFloatCapital':'free_float_capital',
        })
        # 插入数据库
        insert_df_to_postgres(capital_df, table_name='ashare_capital')
    except Exception as err:
        logger_datacube.error(f'[HISTORY] ashare_capital:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'[HISTORY] successfully inserted ashare_capital  data,time cost: {end_time - start_time},lens={len(capital_df)}')


if __name__ == '__main__':
    extract_stock_capital_history()

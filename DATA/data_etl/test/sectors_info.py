import time

import pandas as pd
from tqdm import tqdm
from xtquant import xtdata
import datetime
from Utils.logger import logger_datacube
from Utils.Database_connector import PostgresClient
import akshare as ak

def insert_sectors_info_history(df: pd.DataFrame, table_name: str):
    client = PostgresClient()
    df.to_sql(table_name, con=client.engine, index=False, if_exists='append')


def extract_sectors_info_history():
    start_time = datetime.datetime.now()
    try:
        list_all = xtdata.get_sector_list()
        sectors_info_df = pd.DataFrame()
        count=[1]*7
        for sector in list_all:

            if sector.startswith('TGN'):
                sector_ori = xtdata.get_stock_list_in_sector(sector)
                sector_ori_df = pd.DataFrame(sector_ori,columns=['ticker'])
                sector_ori_df['sector_name'] = sector[3:]
                sector_ori_df['sector_type'] = 'TGN'
                sector_ori_df['sector_num'] = count[0]
                count[0]+=1
            elif sector.startswith('SW'):
                sector_ori = xtdata.get_stock_list_in_sector(sector)
                sector_ori_df = pd.DataFrame(sector_ori,columns=['ticker'])
                sector_ori_df['sector_name'] = sector[3:]
                sector_ori_df['sector_type'] = sector[:3]
                sector_ori_df['sector_num'] = count[int(sector[2])]
                count[int(sector[2])] += 1
            elif sector.startswith('300SW'):
                sector_ori = xtdata.get_stock_list_in_sector(sector)
                sector_ori_df = pd.DataFrame(sector_ori,columns=['ticker'])
                sector_ori_df['sector_name'] = sector[5:]
                sector_ori_df['sector_type'] = sector[:5]
                sector_ori_df['sector_num'] = count[4]
                count[4] += 1
            elif sector.startswith('500SW'):
                sector_ori = xtdata.get_stock_list_in_sector(sector)
                sector_ori_df = pd.DataFrame(sector_ori,columns=['ticker'])
                sector_ori_df['sector_name'] = sector[5:]
                sector_ori_df['sector_type'] = sector[:5]
                sector_ori_df['sector_num'] = count[5]
                count[5] += 1
            elif sector.startswith('1000SW'):
                sector_ori = xtdata.get_stock_list_in_sector(sector)
                sector_ori_df = pd.DataFrame(sector_ori,columns=['ticker'])
                sector_ori_df['sector_name'] = sector[6:]
                sector_ori_df['sector_type'] = sector[:6]
                sector_ori_df['sector_num'] = count[6]
                count[6] += 1
            else:
                continue
            sectors_info_df = pd.concat([sectors_info_df, sector_ori_df],ignore_index=True)

        # 插入数据库

        insert_sectors_info_history(sectors_info_df, table_name='ashare_sectors_info')
    except Exception as err:
        logger_datacube.error(f'[ERROR]:{err}')
        return
    end_time = datetime.datetime.now()

    logger_datacube.info(
        f'Successfully inserted financial sectors_info history data,cost: {end_time - start_time}')


if __name__ == '__main__':
    # 同花顺概念板块行情
    # 同花顺行业板块行情
    # sector cons
    # sector eod price
    # 测试概念板块cons获取
    stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()
    ak.stock_board_industry_info_ths()
    for i in stock_board_concept_name_ths_df['代码'].tolist():
        try:
            stock_board_cons_ths_df = ak.stock_board_cons_ths(symbol=i)
            time.sleep(1)
        except  Exception as err:
            print(i)

    #  测试概念板块日行情获取
    sector_list =stock_board_concept_name_ths_df['概念名称'].to_list()

    for i in sector_list:
        try:
            stock_board_concept_hist_ths_df = ak.stock_board_concept_hist_ths(start_year="2010", symbol=i)
            time.sleep(1)
        except Exception as e:
            print(i)


    # extract_sectors_info_history()
    # 申万行业成分股
    sw_index_first_info_df = ak.sw_index_first_info()
    print(sw_index_first_info_df)
    sw_index_second_info_df = ak.sw_index_second_info()
    print(sw_index_second_info_df)
    sw_index_third_info_df = ak.sw_index_third_info()
    print(sw_index_third_info_df)

    index_component_sw_df = ak.index_component_sw(symbol="801016")
    # 申万日报分析
    ak.index_analysis_daily_sw(symbol="一级行业", start_date="20240401", end_date="20240401").columns
    # 申万日行情
    index_hist_sw_df = ak.index_hist_sw(symbol="801010", period="day")
    # 申万指数权重
    index_component_sw_df = ak.index_component_sw(symbol="801016")

    index_component_sw_df = ak.index_component_sw(symbol="801016")
